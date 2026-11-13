package cl.duoc.vidasalud.analytics.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import cl.duoc.vidasalud.analytics.Eventos;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;

class HistorialServiceMemoriaTest {

    private final HistorialServiceMemoria historial =
            new HistorialServiceMemoria(Clock.fixed(Instant.parse("2026-11-13T15:00:00Z"), ZoneOffset.UTC));

    @Test
    @DisplayName("La linea de tiempo sale ordenada por ocurridoEn y sin duplicados")
    void ordenadoYSinDuplicados() {
        AtencionEvento confirmada = Eventos.cambio(41, "SOLICITADA", "CONFIRMADA", "2026-11-13T14:00:05Z");
        AtencionEvento creada = Eventos.creada(41, 1, "2026-11-13T14:00:00Z");

        assertThat(historial.registrar(confirmada, 2, 1)).isTrue();
        assertThat(historial.registrar(creada, 2, 0)).isTrue();
        assertThat(historial.registrar(creada, 2, 0)).isFalse();

        assertThat(historial.historialDe(41L))
                .extracting(HistorialEntrada::estadoNuevo)
                .containsExactly("SOLICITADA", "CONFIRMADA");
        assertThat(historial.historialDe(99L)).isEmpty();
    }
}
