package cl.duoc.vidasalud.analytics.service;

import java.util.List;

import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;

/**
 * Linea de tiempo de cada atencion: todos sus cambios de estado, en orden.
 *
 * Es una interfaz para separar el consumo de Kafka (listener del grupo vidasalud-historial)
 * del lugar donde se guarda. La implementacion es persistencia/HistorialServiceJpa:
 * Oracle, tabla HISTORIAL_ATENCION, con EVENT_ID UNIQUE como respaldo de la idempotencia.
 * Al pasar de la version en memoria a Oracle, ni el listener ni el controlador cambiaron.
 */
public interface HistorialService {

    /**
     * Agrega un evento a la linea de tiempo de su atencion.
     *
     * @return true si se guardo; false si el eventId ya estaba registrado (duplicado ignorado)
     */
    boolean registrar(AtencionEvento evento, int particion, long offset);

    /** Eventos de una atencion ordenados por ocurridoEn; lista vacia si no hay ninguno. */
    List<HistorialEntrada> historialDe(Long atencionId);
}
