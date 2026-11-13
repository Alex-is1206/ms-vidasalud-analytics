package cl.duoc.vidasalud.analytics.service;

import java.util.Map;

/**
 * Respuesta de GET /api/analitica/resumen: lo que muestra el panel del frontend.
 *
 * @param totalAtenciones             atenciones distintas conocidas
 * @param atencionesPorEstado         cuantas hay hoy en cada estado (SOLICITADA, CONFIRMADA...)
 * @param confirmacionesUltimoMinuto  confirmaciones ocurridas en el ultimo minuto
 * @param confirmacionesUltimaHora    confirmaciones ocurridas en la ultima hora
 * @param atencionesPorPrestacion     atenciones creadas por tipo de prestacion
 * @param boxesTotales                boxes conocidos (topico compactado)
 * @param boxesActivos                boxes disponibles
 * @param capacidadActiva             suma de la capacidad de los boxes activos
 * @param eventosProcesados           eventos aplicados desde que arranco el servicio
 * @param duplicadosIgnorados         eventos descartados por idempotencia
 * @param eventosPorParticion         reparto de eventos de atenciones entre las 3 particiones
 * @param ultimoEvento                ultimo evento de atencion procesado, con particion, offset e hilo
 * @param generadoEn                  momento en que se calculo este resumen
 */
public record ResumenAnalitica(
        long totalAtenciones,
        Map<String, Long> atencionesPorEstado,
        long confirmacionesUltimoMinuto,
        long confirmacionesUltimaHora,
        Map<Long, Long> atencionesPorPrestacion,
        long boxesTotales,
        long boxesActivos,
        long capacidadActiva,
        long eventosProcesados,
        long duplicadosIgnorados,
        Map<Integer, Long> eventosPorParticion,
        MetricasService.UltimoEvento ultimoEvento,
        String generadoEn
) {
}
