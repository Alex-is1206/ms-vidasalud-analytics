package cl.duoc.vidasalud.analytics.kafka.listener;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

/**
 * Lleva el consumidor al inicio de cada particion la PRIMERA vez que se le asigna.
 *
 * Lo usan los listeners que guardan su resultado en memoria (metricas y boxes): al
 * arrancar el servicio releen lo que Kafka retiene y reconstruyen el estado. En una
 * reasignacion posterior (rebalanceo) no se rebobina de nuevo, porque el estado en
 * memoria ya existe; la idempotencia cubre cualquier mensaje repetido.
 *
 * Antes de rebobinar recuerda en que offset iba el grupo en cada particion. Todo lo que
 * este por debajo de ese offset es RELECTURA: ya se proceso antes del reinicio. El listener
 * lo usa para no volver a enviar a la DLT un evento invalido que ya se envio en su momento.
 *
 * Los offsets confirmados del grupo no se pierden: Kafka UI sigue mostrando el lag real.
 */
class RebobinadoInicial {

    private static final Logger log = LoggerFactory.getLogger(RebobinadoInicial.class);

    private final boolean activo;
    /** Particion -> offset en que iba el grupo cuando se rebobino. */
    private final Map<TopicPartition, Long> posicionAntesDeRebobinar = new ConcurrentHashMap<>();

    RebobinadoInicial(boolean activo) {
        this.activo = activo;
    }

    void alAsignar(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
        if (!activo) {
            return;
        }
        List<TopicPartition> nuevas = asignadas.entrySet().stream()
                .filter(e -> posicionAntesDeRebobinar.putIfAbsent(e.getKey(), e.getValue()) == null)
                .map(Map.Entry::getKey)
                .toList();
        if (!nuevas.isEmpty()) {
            callback.seekToBeginning(nuevas);
            log.info("analitica | reconstruyendo estado desde el inicio | particiones {}", nuevas);
        }
    }

    /** true si el mensaje ya se habia procesado antes de este arranque. */
    boolean esRelectura(String topico, int particion, long offset) {
        Long posicion = posicionAntesDeRebobinar.get(new TopicPartition(topico, particion));
        return posicion != null && offset < posicion;
    }
}
