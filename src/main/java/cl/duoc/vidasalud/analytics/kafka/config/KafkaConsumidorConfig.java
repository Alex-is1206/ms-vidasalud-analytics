package cl.duoc.vidasalud.analytics.kafka.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import cl.duoc.vidasalud.analytics.kafka.EventoInvalidoException;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.BoxEstadoEvento;

/**
 * Configuracion del lado consumidor de Kafka.
 *
 * Aqui vive todo lo tecnico de la mensajeria; los listeners solo validan y delegan.
 *
 * 1. Deserializacion segura: ErrorHandlingDeserializer envuelve al deserializador JSON.
 *    Si llega un mensaje que no es JSON valido, no revienta el consumidor en un bucle:
 *    el error se entrega al manejador de errores y el mensaje va a la DLT.
 * 2. Sin cabeceras de tipo: el JSON siempre se convierte al DTO local de este servicio.
 * 3. Commit de offsets por registro (AckMode.RECORD): el contenedor confirma cada mensaje
 *    recien cuando el listener termino bien. enable.auto.commit=false.
 * 4. Errores: 2 reintentos separados por 1 segundo; si siguen fallando, el mensaje se
 *    publica en <topico>.DLT y la particion sigue avanzando. Los errores que no se
 *    arreglan reintentando (JSON malformado, evento invalido) van directo a la DLT.
 */
@Configuration
public class KafkaConsumidorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumidorConfig.class);

    private final NombresKafka nombres;

    public KafkaConsumidorConfig(NombresKafka nombres) {
        this.nombres = nombres;
    }

    // ------------------------------------------------------------------
    // Fabricas de contenedores: una por tipo de evento
    // ------------------------------------------------------------------

    /** Contenedores que leen vidasalud.atenciones.eventos (grupos analitica e historial). */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AtencionEvento> atencionesListenerFactory(
            DefaultErrorHandler manejadorErroresKafka) {
        return fabricaContenedores(AtencionEvento.class, manejadorErroresKafka);
    }

    /** Contenedores que leen el topico compactado vidasalud.boxes.estado. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, BoxEstadoEvento> boxesListenerFactory(
            DefaultErrorHandler manejadorErroresKafka) {
        return fabricaContenedores(BoxEstadoEvento.class, manejadorErroresKafka);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> fabricaContenedores(
            Class<T> tipo, DefaultErrorHandler manejadorErrores) {
        ConcurrentKafkaListenerContainerFactory<String, T> fabrica = new ConcurrentKafkaListenerContainerFactory<>();
        fabrica.setConsumerFactory(fabricaConsumidores(tipo));
        fabrica.setCommonErrorHandler(manejadorErrores);
        fabrica.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return fabrica;
    }

    private <T> ConsumerFactory<String, T> fabricaConsumidores(Class<T> tipo) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nombres.bootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, nombres.clientId());

        // Un grupo nuevo empieza desde el primer mensaje retenido, no solo desde los nuevos
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // El contenedor de Spring confirma el offset cuando el listener termina bien
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Lote maximo por lectura: acota el trabajo pendiente si el consumidor se cae
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        // Clave como texto; valor JSON protegido por ErrorHandlingDeserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);

        // Siempre al DTO local: el productor no envia cabeceras de tipo
        props.put("spring.json.value.default.type", tipo.getName());
        props.put("spring.json.use.type.headers", false);
        props.put("spring.json.trusted.packages", tipo.getPackageName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ------------------------------------------------------------------
    // Manejo de errores: reintentos + Dead Letter Topic
    // ------------------------------------------------------------------

    /**
     * Manejador de errores comun a todos los listeners.
     *
     * - FixedBackOff(intervalo, reintentos): reintenta el mismo mensaje en el mismo
     *   consumidor, sin perder el orden de la particion.
     * - DeadLetterPublishingRecoverer: al agotar los reintentos publica el mensaje en
     *   <topico>.DLT (sufijo configurable), en la misma particion, con cabeceras que explican el error
     *   (kafka_dlt-exception-message, topico y offset originales).
     * - No reintentables: EventoInvalidoException (contrato roto). Los errores de
     *   deserializacion ya vienen marcados como no reintentables por Spring Kafka.
     */
    @Bean
    public DefaultErrorHandler manejadorErroresKafka() {
        // Destino explicito: <topico><sufijo>, misma particion. Spring Kafka 4 usaria "-dlt" por defecto,
        // que no coincide con los topicos .DLT creados por kafka-admin.
        DeadLetterPublishingRecoverer haciaDlt = new DeadLetterPublishingRecoverer(plantillasDlt(),
                (registro, error) -> new TopicPartition(registro.topic() + nombres.sufijoDlt(), registro.partition()));

        ConsumerRecordRecoverer recuperador = (registro, error) -> {
            log.error("analitica | enviado a DLT | topico {} | particion {} | offset {} | key {} | eventId {} | motivo {}",
                    registro.topic(), registro.partition(), registro.offset(), registro.key(),
                    eventIdDe(registro), causaRaiz(error));
            haciaDlt.accept(registro, error);
        };

        DefaultErrorHandler manejador = new DefaultErrorHandler(
                recuperador, new FixedBackOff(nombres.intervaloReintentoMs(), nombres.reintentos()));
        manejador.addNotRetryableExceptions(EventoInvalidoException.class);
        manejador.setRetryListeners((registro, error, intento) ->
                log.warn("analitica | error al procesar | intento {} | topico {} | particion {} | offset {} | motivo {}",
                        intento, registro.topic(), registro.partition(), registro.offset(), causaRaiz(error)));
        return manejador;
    }

    /**
     * Plantillas para publicar en la DLT, elegidas segun el tipo del valor:
     * - byte[]: el mensaje no se pudo deserializar; se reenvian los bytes originales tal cual.
     * - cualquier otro objeto: el DTO se pudo leer pero fallo despues; se reenvia como JSON.
     * El orden importa (LinkedHashMap): byte[] tambien es un Object.
     */
    private Map<Class<?>, KafkaOperations<?, ?>> plantillasDlt() {
        Map<Class<?>, KafkaOperations<?, ?>> plantillas = new LinkedHashMap<>();
        plantillas.put(byte[].class, new KafkaTemplate<>(fabricaProductorDlt(ByteArraySerializer.class)));
        plantillas.put(Object.class, new KafkaTemplate<>(fabricaProductorDlt(JacksonJsonSerializer.class)));
        return plantillas;
    }

    private DefaultKafkaProducerFactory<String, Object> fabricaProductorDlt(Class<?> serializadorValor) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, nombres.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, nombres.clientId() + "-dlt-" + serializadorValor.getSimpleName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializadorValor);
        props.put("spring.json.add.type.headers", false);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        return new DefaultKafkaProducerFactory<>(props);
    }

    // ------------------------------------------------------------------
    // Utilidades para los logs
    // ------------------------------------------------------------------

    static String eventIdDe(ConsumerRecord<?, ?> registro) {
        Object valor = registro.value();
        if (valor instanceof AtencionEvento a) {
            return a.eventId();
        }
        if (valor instanceof BoxEstadoEvento b) {
            return b.eventId();
        }
        return "-";
    }

    static String causaRaiz(Throwable error) {
        Throwable actual = error;
        while (actual.getCause() != null && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        return actual.getClass().getSimpleName() + ": " + actual.getMessage();
    }
}
