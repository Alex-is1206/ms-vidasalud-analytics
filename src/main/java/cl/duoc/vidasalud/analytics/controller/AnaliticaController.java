package cl.duoc.vidasalud.analytics.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.vidasalud.analytics.kafka.evento.BoxEstadoEvento;
import cl.duoc.vidasalud.analytics.service.HistorialEntrada;
import cl.duoc.vidasalud.analytics.service.HistorialService;
import cl.duoc.vidasalud.analytics.service.MetricasService;
import cl.duoc.vidasalud.analytics.service.ResumenAnalitica;

/**
 * API de lectura de la analitica. Solo GET: los datos entran por Kafka, no por HTTP.
 *
 * No conoce Kafka ni la base de datos: solo consulta los servicios.
 * El BFF expone estas rutas al frontend para los roles admin y auditor (tarea A2).
 */
@RestController
@RequestMapping("/api/analitica")
public class AnaliticaController {

    private final MetricasService metricas;
    private final HistorialService historial;

    public AnaliticaController(MetricasService metricas, HistorialService historial) {
        this.metricas = metricas;
        this.historial = historial;
    }

    /** Metricas en tiempo real para el panel. */
    @GetMapping("/resumen")
    public ResumenAnalitica resumen() {
        return metricas.resumen();
    }

    /** Estado actual de cada box, reconstruido desde el topico compactado. */
    @GetMapping("/boxes")
    public List<BoxEstadoEvento> boxes() {
        return metricas.boxes();
    }

    /** Linea de tiempo de una atencion: todos sus cambios de estado en orden. */
    @GetMapping("/atenciones/{id}/historial")
    public ResponseEntity<?> historial(@PathVariable Long id) {
        if (id == null || id <= 0) {
            return ResponseEntity.badRequest()
                    .body(Map.of("campo", "id", "mensaje", "debe ser un numero positivo"));
        }
        List<HistorialEntrada> eventos = historial.historialDe(id);
        if (eventos.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("mensaje", "No hay eventos registrados para la atencion " + id));
        }
        return ResponseEntity.ok(eventos);
    }
}
