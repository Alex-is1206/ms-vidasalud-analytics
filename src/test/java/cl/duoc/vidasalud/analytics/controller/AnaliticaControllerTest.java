package cl.duoc.vidasalud.analytics.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import cl.duoc.vidasalud.analytics.service.HistorialEntrada;
import cl.duoc.vidasalud.analytics.service.HistorialService;
import cl.duoc.vidasalud.analytics.service.MetricasService;

@ExtendWith(MockitoExtension.class)
class AnaliticaControllerTest {

    @Mock
    private MetricasService metricas;
    @Mock
    private HistorialService historial;

    private AnaliticaController controller;

    @BeforeEach
    void setUp() {
        controller = new AnaliticaController(metricas, historial);
    }

    @Test
    @DisplayName("Historial existente: 200 con la linea de tiempo")
    void historialExistente() {
        HistorialEntrada entrada = new HistorialEntrada("e1", "ATENCION_CREADA", 41L, null, "SOLICITADA",
                1L, "2026-11-13T15:00:00Z", "2026-11-13T15:00:01Z", 2, 0);
        when(historial.historialDe(41L)).thenReturn(List.of(entrada));

        ResponseEntity<?> respuesta = controller.historial(41L);

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).isEqualTo(List.of(entrada));
    }

    @Test
    @DisplayName("Atencion sin eventos: 404")
    void historialInexistente() {
        when(historial.historialDe(99L)).thenReturn(List.of());

        assertThat(controller.historial(99L).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("Id no positivo: 400 sin consultar el servicio")
    void idInvalido() {
        assertThat(controller.historial(0L).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        verifyNoInteractions(historial);
    }
}
