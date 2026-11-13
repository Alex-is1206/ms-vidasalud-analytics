package cl.duoc.vidasalud.analytics.persistencia;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.service.HistorialEntrada;
import cl.duoc.vidasalud.analytics.service.HistorialService;

/**
 * Historial de atenciones guardado en Oracle (tabla HISTORIAL_ATENCION).
 *
 * Implementa la misma interfaz que usaba la version en memoria: el listener del grupo
 * vidasalud-historial y el controlador no cambiaron al conectar la base de datos.
 *
 * Idempotencia en dos capas:
 * 1. existsByEventId: el caso normal, evita un INSERT que se sabe repetido.
 * 2. Restriccion UNIQUE sobre EVENT_ID: si dos lecturas del mismo evento llegan casi a la
 *    vez, Oracle rechaza la segunda. Esa excepcion se registra y NO se relanza: si se
 *    relanzara, el manejador de errores la reintentaria y terminaria en la DLT, cuando en
 *    realidad el evento ya esta guardado.
 *
 * Los metodos no son @Transactional a proposito: cada saveAndFlush corre en su propia
 * transaccion (la del repositorio), asi la excepcion por duplicado se puede capturar aqui
 * sin dejar marcada para rollback una transaccion exterior.
 * Cualquier otro error de base de datos (Oracle caido) SI se relanza: el mensaje se
 * reintenta y, si sigue fallando, queda en la DLT para reprocesarlo.
 */
@Service
public class HistorialServiceJpa implements HistorialService {

    private static final Logger log = LoggerFactory.getLogger(HistorialServiceJpa.class);

    private final HistorialAtencionRepository repositorio;
    private final Clock reloj;

    @Autowired
    public HistorialServiceJpa(HistorialAtencionRepository repositorio) {
        this(repositorio, Clock.systemUTC());
    }

    HistorialServiceJpa(HistorialAtencionRepository repositorio, Clock reloj) {
        this.repositorio = repositorio;
        this.reloj = reloj;
    }

    @Override
    public boolean registrar(AtencionEvento evento, int particion, long offset) {
        if (repositorio.existsByEventId(evento.eventId())) {
            return false;
        }
        HistorialAtencion fila = new HistorialAtencion(
                evento.eventId(),
                evento.atencionId(),
                evento.tipo().name(),
                evento.estadoAnterior(),
                evento.estadoNuevo(),
                evento.prestacionId(),
                evento.boxId(),
                Instant.parse(evento.ocurridoEn()),
                reloj.instant(),
                particion,
                offset);
        try {
            repositorio.saveAndFlush(fila);
            return true;
        } catch (DataIntegrityViolationException duplicado) {
            log.info("historial | evento duplicado ignorado por la restriccion unica | eventId {}", evento.eventId());
            return false;
        }
    }

    @Override
    public List<HistorialEntrada> historialDe(Long atencionId) {
        return repositorio.findByAtencionIdOrderByOcurridoEnAscIdAsc(atencionId).stream()
                .map(HistorialServiceJpa::aEntrada)
                .toList();
    }

    private static HistorialEntrada aEntrada(HistorialAtencion f) {
        return new HistorialEntrada(
                f.getEventId(),
                f.getTipo(),
                f.getAtencionId(),
                f.getEstadoAnterior(),
                f.getEstadoNuevo(),
                f.getBoxId(),
                f.getOcurridoEn().toString(),
                f.getProcesadoEn().toString(),
                f.getParticion(),
                f.getOffsetKafka());
    }
}
