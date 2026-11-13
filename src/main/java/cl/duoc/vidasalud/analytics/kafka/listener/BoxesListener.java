package cl.duoc.vidasalud.analytics.kafka.listener;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import cl.duoc.vidasalud.analytics.kafka.EventoInvalidoException;
import cl.duoc.vidasalud.analytics.kafka.ValidadorEventos;
import cl.duoc.vidasalud.analytics.kafka.evento.BoxEstadoEvento;
import cl.duoc.vidasalud.analytics.service.MetricasService;

/**
 * Grupo vidasalud-analitica sobre el topico compactado vidasalud.boxes.estado.
 *
 * Al arrancar relee el topico desde el inicio. Como esta compactado, Kafka conserva el
 * ultimo mensaje de cada box, asi que esa relectura basta para conocer el estado actual
 * de todos los boxes sin consultar la base de datos de catalog.
 */
@Component
public class BoxesListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(BoxesListener.class);

    private final ValidadorEventos validador;
    private final MetricasService metricas;
    private final RebobinadoInicial rebobinado;

    public BoxesListener(ValidadorEventos validador,
                         MetricasService metricas,
                         @Value("${vidasalud.analitica.reconstruir-al-iniciar:true}") boolean reconstruir) {
        this.validador = validador;
        this.metricas = metricas;
        this.rebobinado = new RebobinadoInicial(reconstruir);
    }

    @KafkaListener(
            id = "analitica-boxes",
            idIsGroup = false,
            topics = "${vidasalud.kafka.topico-boxes}",
            groupId = "${vidasalud.kafka.grupo-analitica}",
            containerFactory = "boxesListenerFactory")
    public void escuchar(ConsumerRecord<String, BoxEstadoEvento> registro) {
        BoxEstadoEvento evento = registro.value();
        try {
            validador.validar(evento);
        } catch (EventoInvalidoException e) {
            if (rebobinado.esRelectura(registro.topic(), registro.partition(), registro.offset())) {
                // Ya se envio a la DLT antes del reinicio: no se duplica
                log.info("analitica | relectura: evento invalido ya enviado a la DLT, se omite | particion {} | offset {}",
                        registro.partition(), registro.offset());
                return;
            }
            throw e;
        }

        if (metricas.registrarBox(evento)) {
            log.info("analitica | box {} {} | activo {} | capacidad {} | particion {} | offset {}",
                    evento.boxId(), evento.accion(), evento.activo(), evento.capacidad(),
                    registro.partition(), registro.offset());
        } else {
            log.info("analitica | evento duplicado ignorado | eventId {} | particion {} | offset {}",
                    evento.eventId(), registro.partition(), registro.offset());
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
        rebobinado.alAsignar(asignadas, callback);
    }
}
