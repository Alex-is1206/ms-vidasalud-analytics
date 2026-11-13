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

import cl.duoc.vidasalud.analytics.kafka.ValidadorEventos;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.service.MetricasService;

/**
 * Grupo vidasalud-analitica sobre vidasalud.atenciones.eventos: metricas en tiempo real.
 *
 * concurrency = 3: tres consumidores en el mismo grupo, uno por particion. Kafka reparte
 * las particiones entre ellos y cada uno procesa la suya en orden. El nombre del hilo
 * (analitica-atenciones-0, -1, -2) aparece en el log para mostrar ese reparto.
 *
 * El listener no tiene logica de negocio: valida, delega en MetricasService y registra.
 * Si lanza una excepcion, el DefaultErrorHandler decide si reintentar o enviar a la DLT.
 */
@Component
public class AtencionesAnaliticaListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(AtencionesAnaliticaListener.class);

    private final ValidadorEventos validador;
    private final MetricasService metricas;
    private final RebobinadoInicial rebobinado;

    public AtencionesAnaliticaListener(ValidadorEventos validador,
                                       MetricasService metricas,
                                       @Value("${vidasalud.analitica.reconstruir-al-iniciar:true}") boolean reconstruir) {
        this.validador = validador;
        this.metricas = metricas;
        this.rebobinado = new RebobinadoInicial(reconstruir);
    }

    @KafkaListener(
            id = "analitica-atenciones",
            idIsGroup = false,
            topics = "${vidasalud.kafka.topico-atenciones}",
            groupId = "${vidasalud.kafka.grupo-analitica}",
            concurrency = "3",
            containerFactory = "atencionesListenerFactory")
    public void escuchar(ConsumerRecord<String, AtencionEvento> registro) {
        AtencionEvento evento = registro.value();
        validador.validar(evento);

        String hilo = Thread.currentThread().getName();
        boolean aplicado = metricas.registrarAtencion(evento, registro.partition(), registro.offset(), hilo);

        if (aplicado) {
            log.info("analitica | {} {} -> {} | atencion {} | particion {} | offset {}",
                    evento.tipo(), evento.estadoAnterior(), evento.estadoNuevo(),
                    evento.atencionId(), registro.partition(), registro.offset());
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
