package cl.duoc.vidasalud.analytics.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import cl.duoc.vidasalud.analytics.Eventos;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;

class MetricasServiceTest {

    /** "Ahora" fijo para que las ventanas de tiempo sean predecibles. */
    private static final Instant AHORA = Instant.parse("2026-11-13T15:00:00Z");

    private MetricasService metricas;

    @BeforeEach
    void setUp() {
        metricas = new MetricasService(Clock.fixed(AHORA, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Crear y confirmar mueve la atencion de SOLICITADA a CONFIRMADA")
    void creacionYConfirmacionActualizanContadores() {
        metricas.registrarAtencion(Eventos.creada(41, 3, "2026-11-13T14:59:50Z"), 2, 0, "hilo-0");
        metricas.registrarAtencion(Eventos.cambio(41, "SOLICITADA", "CONFIRMADA", "2026-11-13T14:59:55Z"), 2, 1, "hilo-0");

        ResumenAnalitica r = metricas.resumen();
        assertThat(r.totalAtenciones()).isEqualTo(1);
        assertThat(r.atencionesPorEstado()).containsEntry("CONFIRMADA", 1L).doesNotContainKey("SOLICITADA");
        assertThat(r.confirmacionesUltimoMinuto()).isEqualTo(1);
        assertThat(r.confirmacionesUltimaHora()).isEqualTo(1);
        assertThat(r.atencionesPorPrestacion()).containsEntry(3L, 1L);
        assertThat(r.eventosPorParticion()).containsEntry(2, 2L);
        assertThat(r.ultimoEvento().offset()).isEqualTo(1);
        assertThat(r.ultimoEvento().estadoNuevo()).isEqualTo("CONFIRMADA");
    }

    @Test
    @DisplayName("Un evento repetido (mismo eventId) se ignora: idempotencia")
    void duplicadoSeIgnora() {
        AtencionEvento evento = Eventos.cambio(7, "SOLICITADA", "CONFIRMADA", "2026-11-13T14:59:00Z");

        assertThat(metricas.registrarAtencion(evento, 0, 5, "hilo")).isTrue();
        assertThat(metricas.registrarAtencion(evento, 0, 5, "hilo")).isFalse();

        ResumenAnalitica r = metricas.resumen();
        assertThat(r.confirmacionesUltimaHora()).isEqualTo(1);
        assertThat(r.eventosProcesados()).isEqualTo(1);
        assertThat(r.duplicadosIgnorados()).isEqualTo(1);
    }

    @Test
    @DisplayName("Las confirmaciones se cuentan por ventana: ultimo minuto y ultima hora")
    void ventanasDeTiempo() {
        metricas.registrarAtencion(Eventos.cambio(1, "SOLICITADA", "CONFIRMADA", "2026-11-13T14:59:30Z"), 0, 0, "h");
        metricas.registrarAtencion(Eventos.cambio(2, "SOLICITADA", "CONFIRMADA", "2026-11-13T14:30:00Z"), 1, 0, "h");
        metricas.registrarAtencion(Eventos.cambio(3, "SOLICITADA", "CONFIRMADA", "2026-11-13T12:00:00Z"), 2, 0, "h");

        ResumenAnalitica r = metricas.resumen();
        assertThat(r.confirmacionesUltimoMinuto()).isEqualTo(1);
        assertThat(r.confirmacionesUltimaHora()).isEqualTo(2);
        // El estado actual si cuenta las tres, aunque la confirmacion sea antigua
        assertThat(r.atencionesPorEstado()).containsEntry("CONFIRMADA", 3L);
    }

    @Test
    @DisplayName("Boxes: se conserva el ultimo estado de cada box (como el topico compactado)")
    void ultimoEstadoDeCadaBox() {
        metricas.registrarBox(Eventos.box(1, 2, true));
        metricas.registrarBox(Eventos.box(2, 3, true));
        metricas.registrarBox(Eventos.box(2, 3, false));

        ResumenAnalitica r = metricas.resumen();
        assertThat(r.boxesTotales()).isEqualTo(2);
        assertThat(r.boxesActivos()).isEqualTo(1);
        assertThat(r.capacidadActiva()).isEqualTo(2);
        assertThat(metricas.boxes()).extracting(b -> b.boxId()).containsExactly(1L, 2L);
    }
}
