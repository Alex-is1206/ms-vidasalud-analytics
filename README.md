# ms-vidasalud-analytics

Microservicio de **analítica en tiempo real** de VidaSalud. Consume los eventos que publican `ms-vidasalud-appointments` y `ms-vidasalud-catalog` en **Kafka** y con ellos:

- mantiene **métricas en vivo** (atenciones por estado, confirmaciones por minuto y hora, boxes activos),
- guarda la **línea de tiempo** de cada atención (historial) en **Oracle**, tabla `HISTORIAL_ATENCION`.

No publica eventos de negocio y no reemplaza a RabbitMQ: RabbitMQ sigue repartiendo **tareas** (avisar, generar ticket, auditar); Kafka guarda el **registro de lo que pasó** para que varios lo lean a su ritmo.

| | |
|---|---|
| Puerto | `8086` (en Docker no se publica: el BFF lo consulta por la red interna) |
| Java / Spring Boot | 21 / 4.1.1 |
| Paquete | `cl.duoc.vidasalud.analytics` |

---

## Tópicos y grupos de consumidores

| Tópico | Clave | Grupo | Hilos | Para qué |
|---|---|---|---|---|
| `vidasalud.atenciones.eventos` | `atencionId` | `vidasalud-analitica` | **3** (uno por partición) | Métricas en memoria |
| `vidasalud.atenciones.eventos` | `atencionId` | `vidasalud-historial` | 1 | Línea de tiempo de cada atención, en Oracle |
| `vidasalud.boxes.estado` (compactado) | `boxId` | `vidasalud-analitica` | 1 | Estado actual de cada box |

- **Mismo tópico, dos grupos:** cada grupo recibe **todos** los eventos y lleva sus propios offsets. Si el historial se atrasa, las métricas no se enteran.
- **Mismo grupo, tres hilos:** Kafka reparte las 3 particiones entre los 3 consumidores de `vidasalud-analitica`. Como la clave es `atencionId`, todos los eventos de una atención caen en la misma partición y se procesan **en orden**.
- **Reconstrucción al iniciar:** las métricas viven en memoria, así que al arrancar el grupo de analítica relee los tópicos desde el inicio (7 días de retención en atenciones, último estado por box en el compactado). El historial **no** relee: continúa desde su último offset confirmado.

## Estructura

```
kafka/config/      KafkaConsumidorConfig   deserialización segura, commit por registro, reintentos y DLT
                   NombresKafka            nombres de tópicos y grupos desde application.properties
kafka/evento/      AtencionEvento, BoxEstadoEvento (+ enums)   contrato, copia local del productor
kafka/listener/    AtencionesAnaliticaListener, AtencionesHistorialListener, BoxesListener
kafka/             ValidadorEventos, EventoInvalidoException
service/           MetricasService (memoria + idempotencia LRU), HistorialService (interfaz)
persistencia/      HistorialAtencion (entidad), HistorialAtencionRepository, HistorialServiceJpa
controller/        AnaliticaController
sql/               historial_atencion.sql   definición equivalente de la tabla
```

Los **listeners no tienen lógica de negocio**: validan el evento, llaman al servicio y registran el resultado. Si algo falla lanzan la excepción y el manejador de errores decide qué hacer.

## Manejo de errores

| Situación | Qué pasa |
|---|---|
| El mensaje no es JSON | `ErrorHandlingDeserializer` captura el error → **directo a `<tópico>.DLT`**, sin reintentos, con los bytes originales |
| JSON válido pero rompe el contrato (faltan campos, fecha ilegible, tipo incoherente) | `EventoInvalidoException` (no reintentable) → **directo a la DLT** |
| Cualquier otro error (pasajero) | `DefaultErrorHandler` con `FixedBackOff(1000 ms, 2)`: **2 reintentos** → si sigue fallando, a la DLT |
| Evento repetido (mismo `eventId`) | Se ignora: idempotencia (LRU de 10 000 ids en métricas, `EVENT_ID` UNIQUE en Oracle para el historial) |
| Oracle caído al guardar el historial | La excepción se propaga → 2 reintentos → DLT. El grupo de métricas no se ve afectado |

En todos los casos la partición **sigue avanzando**: un mensaje malo no bloquea a los que vienen detrás. Cada envío a la DLT deja un log:

```
analitica | enviado a DLT | topico vidasalud.atenciones.eventos | particion 1 | offset 12 | key 98 | eventId ... | motivo EventoInvalidoException: ...
```

La DLT conserva el mensaje original y Spring Kafka le agrega cabeceras con el motivo (`kafka_dlt-exception-message`), el tópico, la partición y el offset originales.

## Historial en Oracle

Tabla `HISTORIAL_ATENCION` en el esquema `VIDASALUD` (el mismo Oracle de appointments y catalog). Hibernate la crea al arrancar; la definición equivalente está en `sql/historial_atencion.sql`.

| Columna | Para qué |
|---|---|
| `EVENT_ID` (UNIQUE) | Idempotencia que sobrevive a los reinicios: Oracle rechaza un evento repetido y el servicio lo registra como duplicado sin lanzar error |
| `ATENCION_ID` (indexada) | Consulta de la línea de tiempo |
| `TIPO`, `ESTADO_ANTERIOR`, `ESTADO_NUEVO`, `PRESTACION_ID`, `BOX_ID` | El cambio en sí |
| `OCURRIDO_EN` / `PROCESADO_EN` | Cuándo pasó en appointments y cuándo lo guardó analytics |
| `PARTICION`, `OFFSET_KAFKA` | Traza exacta del mensaje de origen en Kafka |

El historial **no relee** el tópico al arrancar (las métricas sí): continúa desde el último offset confirmado de `vidasalud-historial`, porque lo anterior ya está en Oracle.

## API

Solo lectura. Los datos entran por Kafka, no por HTTP.

| Método | Ruta | Respuesta |
|---|---|---|
| GET | `/api/analitica/resumen` | 200: métricas en vivo |
| GET | `/api/analitica/boxes` | 200: estado actual de cada box |
| GET | `/api/analitica/atenciones/{id}/historial` | 200: línea de tiempo · 404: sin eventos · 400: id no positivo |
| GET | `/actuator/health` | 200 `{"status":"UP"}` |

Ejemplo de `/api/analitica/resumen`:

```json
{
  "totalAtenciones": 12,
  "atencionesPorEstado": { "CANCELADA": 1, "CONFIRMADA": 7, "SOLICITADA": 4 },
  "confirmacionesUltimoMinuto": 1,
  "confirmacionesUltimaHora": 5,
  "atencionesPorPrestacion": { "1": 8, "2": 4 },
  "boxesTotales": 3, "boxesActivos": 2, "capacidadActiva": 3,
  "eventosProcesados": 27, "duplicadosIgnorados": 0,
  "eventosPorParticion": { "0": 9, "1": 6, "2": 12 },
  "ultimoEvento": {
    "tipo": "ESTADO_CAMBIADO", "atencionId": 41,
    "estadoAnterior": "SOLICITADA", "estadoNuevo": "CONFIRMADA",
    "particion": 2, "offset": 1, "hilo": "analitica-atenciones-2-C-1"
  },
  "generadoEn": "2026-11-13T15:20:00Z"
}
```

## Ejecutar en local (PowerShell)

Requiere el clúster de Kafka (`vidasalud-deploy/kafka`) y Oracle encendidos.

```powershell
$env:DB_PASSWORD='<clave de Oracle>'
.\mvnw spring-boot:run
```

Por defecto se conecta a Kafka en `localhost:9092,9093,9094` y a Oracle en `localhost:1521/XEPDB1` con el usuario `vidasalud`. En Docker se usan `KAFKA_BOOTSTRAP=kafka1:29092,kafka2:29092,kafka3:29092` y `DB_URL=jdbc:oracle:thin:@//oracle:1521/XEPDB1`.

```powershell
Invoke-RestMethod http://localhost:8086/api/analitica/resumen
Invoke-RestMethod http://localhost:8086/api/analitica/atenciones/41/historial
```

## Probar la DLT

Se publica a mano en el tópico, desde el contenedor `kafka1`:

```powershell
# 1. Mensaje que no es JSON
Get-Content pruebas\mensaje-malformado.txt | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:29092 --topic vidasalud.atenciones.eventos --property parse.key=true --property "key.separator=|"

# 2. JSON válido que rompe el contrato (ESTADO_CAMBIADO sin estadoAnterior)
Get-Content pruebas\evento-invalido.txt | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:29092 --topic vidasalud.atenciones.eventos --property parse.key=true --property "key.separator=|"
```

En el log aparece `enviado a DLT` **dos veces por mensaje**: una por el grupo de analítica y otra por el de historial, porque cada grupo lee todo. En Kafka UI (`http://localhost:8090`) → Topics → `vidasalud.atenciones.eventos.DLT` → Messages se ven los mensajes con sus cabeceras de error.

## Reprocesar el historial desde el inicio

Con analytics **detenido** (un grupo con miembros activos no se puede mover):

```powershell
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --group vidasalud-historial --topic vidasalud.atenciones.eventos --reset-offsets --to-earliest --execute
```

Al volver a encender analytics, el grupo relee todo: los eventos que ya están en Oracle se ignoran por la restricción única y solo se agregan los que faltaban.

## Pruebas

```powershell
.\mvnw clean test
```

Sin broker ni base de datos: métricas (contadores, ventanas de tiempo, idempotencia, boxes), validador, listeners (delegan, rechazan inválidos, rebobinan una sola vez), historial en Oracle con el repositorio simulado (inserta, ignora duplicados, propaga caídas de la base) y controlador.

## Docker

```powershell
docker build -t ms-vidasalud-analytics .
```

El servicio `analytics` del `docker-compose.yml` de `vidasalud-deploy` lo construye desde `../ms-vidasalud-analytics`.
