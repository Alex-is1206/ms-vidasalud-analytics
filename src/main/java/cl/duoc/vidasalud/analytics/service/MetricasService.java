package cl.duoc.vidasalud.analytics.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.BoxEstadoEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.TipoEventoAtencion;

/**
 * Metricas en tiempo real calculadas a partir de los eventos de Kafka.
 *
 * Vive en memoria porque se puede reconstruir: al iniciar, el grupo vidasalud-analitica
 * relee los topicos desde el principio (7 dias de atenciones y el topico compactado
 * de boxes) y vuelve a llegar al mismo resultado.
 *
 * Idempotencia: recuerda los ultimos eventId procesados (LRU) y descarta los repetidos,
 * asi una relectura o un reenvio del productor no cuenta dos veces lo mismo.
 *
 * Concurrencia: el listener de atenciones corre con 3 hilos (uno por particion), por eso
 * los metodos publicos son synchronized. El volumen de una clinica no hace de esto un cuello
 * de botella.
 *
 * No sabe nada de Kafka: recibe eventos ya validados y la particion y el offset solo
 * para mostrarlos en el resumen.
 */
@Service
public class MetricasService {

    static final int CAPACIDAD_IDEMPOTENCIA = 10_000;
    static final String CONFIRMADA = "CONFIRMADA";

    private final Clock reloj;

    /** Estado actual de cada atencion: permite mover el contador del estado anterior al nuevo. */
    private final Map<Long, String> estadoPorAtencion = new HashMap<>();
    /** Momentos de las confirmaciones de la ultima hora (ventana deslizante). */
    private final Deque<Instant> confirmaciones = new ArrayDeque<>();
    private final Map<Long, Long> atencionesPorPrestacion = new TreeMap<>();
    private final Map<Integer, Long> eventosPorParticion = new TreeMap<>();
    /** Ultimo estado conocido de cada box (lo que conserva el topico compactado). */
    private final Map<Long, BoxEstadoEvento> boxes = new TreeMap<>();

    private final Map<String, Boolean> eventosVistos = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> masAntiguo) {
            return size() > CAPACIDAD_IDEMPOTENCIA;
        }
    };

    private long eventosProcesados;
    private long duplicadosIgnorados;
    private UltimoEvento ultimoEvento;

    @Autowired
    public MetricasService() {
        this(Clock.systemUTC());
    }

    /** Para las pruebas: permite fijar la hora actual. */
    MetricasService(Clock reloj) {
        this.reloj = reloj;
    }

    /**
     * Aplica un evento de atencion a las metricas.
     *
     * @return true si se aplico; false si era un duplicado y se ignoro
     */
    public synchronized boolean registrarAtencion(AtencionEvento evento, int particion, long offset, String hilo) {
        if (yaVisto(evento.eventId())) {
            return false;
        }

        String estadoPrevio = estadoPorAtencion.put(evento.atencionId(), evento.estadoNuevo());
        if (estadoPrevio == null && evento.tipo() == TipoEventoAtencion.ATENCION_CREADA) {
            atencionesPorPrestacion.merge(evento.prestacionId(), 1L, Long::sum);
        }

        Instant ocurrido = Instant.parse(evento.ocurridoEn());
        if (CONFIRMADA.equals(evento.estadoNuevo()) && !CONFIRMADA.equals(estadoPrevio)) {
            confirmaciones.addLast(ocurrido);
        }
        descartarConfirmacionesAntiguas();

        eventosPorParticion.merge(particion, 1L, Long::sum);
        eventosProcesados++;
        ultimoEvento = new UltimoEvento(evento.eventId(), evento.tipo().name(), evento.atencionId(),
                evento.estadoAnterior(), evento.estadoNuevo(), evento.ocurridoEn(),
                particion, offset, hilo, reloj.instant().toString());
        return true;
    }

    /**
     * Guarda el ultimo estado de un box.
     *
     * @return true si se aplico; false si era un duplicado y se ignoro
     */
    public synchronized boolean registrarBox(BoxEstadoEvento evento) {
        if (yaVisto(evento.eventId())) {
            return false;
        }
        boxes.put(evento.boxId(), evento);
        eventosProcesados++;
        return true;
    }

    /** Foto de todas las metricas en este instante, para GET /api/analitica/resumen. */
    public synchronized ResumenAnalitica resumen() {
        descartarConfirmacionesAntiguas();

        Map<String, Long> porEstado = new TreeMap<>();
        estadoPorAtencion.values().forEach(estado -> porEstado.merge(estado, 1L, Long::sum));

        Instant haceUnMinuto = reloj.instant().minus(Duration.ofMinutes(1));
        long ultimoMinuto = confirmaciones.stream().filter(m -> m.isAfter(haceUnMinuto)).count();

        long boxesActivos = boxes.values().stream().filter(b -> Boolean.TRUE.equals(b.activo())).count();
        long capacidadActiva = boxes.values().stream()
                .filter(b -> Boolean.TRUE.equals(b.activo()))
                .mapToLong(BoxEstadoEvento::capacidad)
                .sum();

        return new ResumenAnalitica(
                estadoPorAtencion.size(),
                porEstado,
                ultimoMinuto,
                confirmaciones.size(),
                new TreeMap<>(atencionesPorPrestacion),
                boxes.size(),
                boxesActivos,
                capacidadActiva,
                eventosProcesados,
                duplicadosIgnorados,
                new TreeMap<>(eventosPorParticion),
                ultimoEvento,
                reloj.instant().toString());
    }

    /** Estado actual de cada box, reconstruido desde el topico compactado. */
    public synchronized List<BoxEstadoEvento> boxes() {
        List<BoxEstadoEvento> lista = new ArrayList<>(boxes.values());
        lista.sort(Comparator.comparing(BoxEstadoEvento::boxId));
        return lista;
    }

    private boolean yaVisto(String eventId) {
        if (eventosVistos.containsKey(eventId)) {
            duplicadosIgnorados++;
            return true;
        }
        eventosVistos.put(eventId, Boolean.TRUE);
        return false;
    }

    private void descartarConfirmacionesAntiguas() {
        Instant haceUnaHora = reloj.instant().minus(Duration.ofHours(1));
        confirmaciones.removeIf(momento -> !momento.isAfter(haceUnaHora));
    }

    /** Ultimo evento de atencion aplicado: muestra en que particion, offset e hilo se proceso. */
    public record UltimoEvento(
            String eventId,
            String tipo,
            Long atencionId,
            String estadoAnterior,
            String estadoNuevo,
            String ocurridoEn,
            int particion,
            long offset,
            String hilo,
            String procesadoEn) {
    }
}
