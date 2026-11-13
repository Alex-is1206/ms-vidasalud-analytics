package cl.duoc.vidasalud.analytics.persistencia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;

import cl.duoc.vidasalud.analytics.Eventos;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.service.HistorialEntrada;

/** Pruebas del historial en Oracle sin base de datos: se simula el repositorio. */
@ExtendWith(MockitoExtension.class)
class HistorialServiceJpaTest {

    private static final Instant AHORA = Instant.parse("2026-11-13T15:30:00Z");

    @Mock
    private HistorialAtencionRepository repositorio;

    private HistorialServiceJpa servicio;

    @BeforeEach
    void setUp() {
        servicio = new HistorialServiceJpa(repositorio, Clock.fixed(AHORA, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Evento nuevo: se guarda una fila con todos los campos, la particion y el offset")
    void eventoNuevoSeGuarda() {
        AtencionEvento evento = Eventos.cambio(41, "SOLICITADA", "CONFIRMADA", "2026-11-13T15:15:08Z");
        when(repositorio.existsByEventId(evento.eventId())).thenReturn(false);

        assertThat(servicio.registrar(evento, 2, 1L)).isTrue();

        ArgumentCaptor<HistorialAtencion> fila = ArgumentCaptor.forClass(HistorialAtencion.class);
        verify(repositorio).saveAndFlush(fila.capture());
        HistorialAtencion guardada = fila.getValue();
        assertThat(guardada.getEventId()).isEqualTo(evento.eventId());
        assertThat(guardada.getAtencionId()).isEqualTo(41L);
        assertThat(guardada.getTipo()).isEqualTo("ESTADO_CAMBIADO");
        assertThat(guardada.getEstadoAnterior()).isEqualTo("SOLICITADA");
        assertThat(guardada.getEstadoNuevo()).isEqualTo("CONFIRMADA");
        assertThat(guardada.getOcurridoEn()).isEqualTo(Instant.parse("2026-11-13T15:15:08Z"));
        assertThat(guardada.getProcesadoEn()).isEqualTo(AHORA);
        assertThat(guardada.getParticion()).isEqualTo(2);
        assertThat(guardada.getOffsetKafka()).isEqualTo(1L);
    }

    @Test
    @DisplayName("Evento ya registrado: no se inserta de nuevo")
    void duplicadoConocidoNoSeInserta() {
        AtencionEvento evento = Eventos.creada(41, 1, "2026-11-13T15:15:02Z");
        when(repositorio.existsByEventId(evento.eventId())).thenReturn(true);

        assertThat(servicio.registrar(evento, 2, 0L)).isFalse();

        verify(repositorio, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Duplicado detectado por la restriccion UNIQUE: se ignora sin lanzar excepcion")
    void duplicadoPorRestriccionUnicaSeIgnora() {
        AtencionEvento evento = Eventos.creada(41, 1, "2026-11-13T15:15:02Z");
        when(repositorio.existsByEventId(evento.eventId())).thenReturn(false);
        when(repositorio.saveAndFlush(any())).thenThrow(new DataIntegrityViolationException("ORA-00001"));

        assertThat(servicio.registrar(evento, 2, 0L)).isFalse();
    }

    @Test
    @DisplayName("Oracle caido: la excepcion se propaga para que el mensaje se reintente o vaya a la DLT")
    void errorDeBaseDeDatosSePropaga() {
        AtencionEvento evento = Eventos.creada(41, 1, "2026-11-13T15:15:02Z");
        when(repositorio.existsByEventId(evento.eventId()))
                .thenThrow(new DataAccessResourceFailureException("sin conexion"));

        assertThatThrownBy(() -> servicio.registrar(evento, 2, 0L))
                .isInstanceOf(DataAccessResourceFailureException.class);
    }

    @Test
    @DisplayName("La linea de tiempo se devuelve en el orden del repositorio y con las fechas como texto ISO")
    void historialMapeaFilas() {
        HistorialAtencion creada = new HistorialAtencion("e1", 41L, "ATENCION_CREADA", null, "SOLICITADA",
                1L, 1L, Instant.parse("2026-11-13T15:15:02Z"), AHORA, 2, 0L);
        HistorialAtencion confirmada = new HistorialAtencion("e2", 41L, "ESTADO_CAMBIADO", "SOLICITADA", "CONFIRMADA",
                1L, 1L, Instant.parse("2026-11-13T15:15:08Z"), AHORA, 2, 1L);
        when(repositorio.findByAtencionIdOrderByOcurridoEnAscIdAsc(41L)).thenReturn(List.of(creada, confirmada));

        List<HistorialEntrada> linea = servicio.historialDe(41L);

        assertThat(linea).extracting(HistorialEntrada::estadoNuevo).containsExactly("SOLICITADA", "CONFIRMADA");
        assertThat(linea.get(1).ocurridoEn()).isEqualTo("2026-11-13T15:15:08Z");
        assertThat(linea.get(1).offset()).isEqualTo(1L);
    }
}
