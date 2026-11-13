package cl.duoc.vidasalud.analytics.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import cl.duoc.vidasalud.analytics.kafka.ValidadorEventos;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.service.HistorialService;

/**
 * Grupo vidasalud-historial sobre el MISMO topico vidasalud.atenciones.eventos.
 *
 * Al ser otro grupo, recibe todos los eventos de nuevo, con sus propios offsets e
 * independiente del grupo de analitica: si este se atrasa, las metricas no se enteran.
 *
 * Un solo consumidor (concurrency por defecto = 1) lee las 3 particiones: guardar el
 * historial no necesita mas paralelismo. A diferencia de las metricas, NO relee desde
 * el inicio al arrancar: el historial se guarda en Oracle y continua desde
 * el ultimo offset confirmado.
 */
@Component
public class AtencionesHistorialListener {

    private static final Logger log = LoggerFactory.getLogger(AtencionesHistorialListener.class);

    private final ValidadorEventos validador;
    private final HistorialService historial;

    public AtencionesHistorialListener(ValidadorEventos validador, HistorialService historial) {
        this.validador = validador;
        this.historial = historial;
    }

    @KafkaListener(
            id = "historial-atenciones",
            idIsGroup = false,
            topics = "${vidasalud.kafka.topico-atenciones}",
            groupId = "${vidasalud.kafka.grupo-historial}",
            containerFactory = "atencionesListenerFactory")
    public void escuchar(ConsumerRecord<String, AtencionEvento> registro) {
        AtencionEvento evento = registro.value();
        validador.validar(evento);

        boolean guardado = historial.registrar(evento, registro.partition(), registro.offset());

        if (guardado) {
            log.info("historial | atencion {} | {} -> {} | particion {} | offset {}",
                    evento.atencionId(), evento.estadoAnterior(), evento.estadoNuevo(),
                    registro.partition(), registro.offset());
        } else {
            log.info("historial | evento duplicado ignorado | eventId {} | particion {} | offset {}",
                    evento.eventId(), registro.partition(), registro.offset());
        }
    }
}
