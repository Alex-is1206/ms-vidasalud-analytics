package cl.duoc.vidasalud.analytics.kafka.listener;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Los offsets confirmados del grupo no se pierden: Kafka UI sigue mostrando el lag real.
 */
class RebobinadoInicial {

    private static final Logger log = LoggerFactory.getLogger(RebobinadoInicial.class);

    private final boolean activo;
    private final Set<TopicPartition> yaRebobinadas = ConcurrentHashMap.newKeySet();

    RebobinadoInicial(boolean activo) {
        this.activo = activo;
    }

    void alAsignar(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
        if (!activo) {
            return;
        }
        List<TopicPartition> nuevas = asignadas.keySet().stream()
                .filter(yaRebobinadas::add)
                .toList();
        if (!nuevas.isEmpty()) {
            callback.seekToBeginning(nuevas);
            log.info("analitica | reconstruyendo estado desde el inicio | particiones {}", nuevas);
        }
    }
}
