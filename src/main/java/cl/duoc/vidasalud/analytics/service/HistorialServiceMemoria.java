package cl.duoc.vidasalud.analytics.service;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;

/**
 * Implementacion TEMPORAL del historial, en memoria.
 *
 * Sirve para probar el grupo vidasalud-historial antes de conectar Oracle. Se pierde
 * al reiniciar el servicio. En la tarea A1 se BORRA este archivo y se agrega
 * persistencia/HistorialServiceJpa, que implementa la misma interfaz: el listener
 * y el controlador no cambian.
 */
@Service
public class HistorialServiceMemoria implements HistorialService {

    private final Clock reloj;
    private final Map<Long, List<HistorialEntrada>> porAtencion = new HashMap<>();
    private final Set<String> eventIds = new HashSet<>();

    @Autowired
    public HistorialServiceMemoria() {
        this(Clock.systemUTC());
    }

    HistorialServiceMemoria(Clock reloj) {
        this.reloj = reloj;
    }

    @Override
    public synchronized boolean registrar(AtencionEvento evento, int particion, long offset) {
        if (!eventIds.add(evento.eventId())) {
            return false;
        }
        HistorialEntrada entrada = new HistorialEntrada(
                evento.eventId(),
                evento.tipo().name(),
                evento.atencionId(),
                evento.estadoAnterior(),
                evento.estadoNuevo(),
                evento.boxId(),
                evento.ocurridoEn(),
                reloj.instant().toString(),
                particion,
                offset);
        porAtencion.computeIfAbsent(evento.atencionId(), id -> new ArrayList<>()).add(entrada);
        return true;
    }

    @Override
    public synchronized List<HistorialEntrada> historialDe(Long atencionId) {
        List<HistorialEntrada> lista = new ArrayList<>(porAtencion.getOrDefault(atencionId, List.of()));
        lista.sort(Comparator.comparing((HistorialEntrada e) -> Instant.parse(e.ocurridoEn())));
        return lista;
    }
}
