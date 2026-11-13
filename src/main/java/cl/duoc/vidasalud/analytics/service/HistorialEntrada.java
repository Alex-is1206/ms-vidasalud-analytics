package cl.duoc.vidasalud.analytics.service;

/**
 * Un punto de la linea de tiempo de una atencion.
 * Coincide con las columnas de HISTORIAL_ATENCION en Oracle (tarea A1).
 */
public record HistorialEntrada(
        String eventId,
        String tipo,
        Long atencionId,
        String estadoAnterior,
        String estadoNuevo,
        Long boxId,
        String ocurridoEn,
        String procesadoEn,
        int particion,
        long offset
) {
}
