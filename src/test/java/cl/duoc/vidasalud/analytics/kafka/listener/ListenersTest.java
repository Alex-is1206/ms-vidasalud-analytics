package cl.duoc.vidasalud.analytics.kafka.listener;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import cl.duoc.vidasalud.analytics.Eventos;
import cl.duoc.vidasalud.analytics.kafka.EventoInvalidoException;
import cl.duoc.vidasalud.analytics.kafka.ValidadorEventos;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.BoxEstadoEvento;
import cl.duoc.vidasalud.analytics.service.HistorialService;
import cl.duoc.vidasalud.analytics.service.MetricasService;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;

/**
 * Pruebas de los listeners sin broker: se arma el ConsumerRecord a mano y se simulan
 * los servicios. Comprueban que el listener valida, delega y deja pasar la excepcion
 * al manejador de errores (que es quien decide reintento o DLT).
 */
@ExtendWith(MockitoExtension.class)
class ListenersTest {

    private static final String TOPICO = "vidasalud.atenciones.eventos";

    private static ValidatorFactory fabrica;
    private static ValidadorEventos validador;

    @Mock
    private MetricasService metricas;
    @Mock
    private HistorialService historial;
    @Mock
    private ConsumerSeekCallback callback;

    private AtencionesAnaliticaListener analitica;
    private AtencionesHistorialListener historialListener;
    private BoxesListener boxes;

    @BeforeAll
    static void crearValidador() {
        fabrica = Validation.buildDefaultValidatorFactory();
        validador = new ValidadorEventos(fabrica.getValidator());
    }

    @AfterAll
    static void cerrar() {
        fabrica.close();
    }

    @BeforeEach
    void setUp() {
        analitica = new AtencionesAnaliticaListener(validador, metricas, true);
        historialListener = new AtencionesHistorialListener(validador, historial);
        boxes = new BoxesListener(validador, metricas, true);
    }

    private ConsumerRecord<String, AtencionEvento> registro(AtencionEvento evento) {
        return new ConsumerRecord<>(TOPICO, 2, 7L, "41", evento);
    }

    @Test
    @DisplayName("Analitica: un evento valido se entrega a MetricasService con su particion y offset")
    void analiticaDelegaEventoValido() {
        AtencionEvento evento = Eventos.cambio(41, "SOLICITADA", "CONFIRMADA", "2026-11-13T15:00:00Z");
        when(metricas.registrarAtencion(eq(evento), eq(2), eq(7L), anyString())).thenReturn(true);

        analitica.escuchar(registro(evento));

        verify(metricas).registrarAtencion(eq(evento), eq(2), eq(7L), anyString());
    }

    @Test
    @DisplayName("Analitica: un evento invalido lanza EventoInvalidoException y no toca las metricas")
    void analiticaRechazaEventoInvalido() {
        AtencionEvento invalido = Eventos.cambio(41, null, "CONFIRMADA", "2026-11-13T15:00:00Z");

        assertThatThrownBy(() -> analitica.escuchar(registro(invalido)))
                .isInstanceOf(EventoInvalidoException.class);
        verifyNoInteractions(metricas);
    }

    @Test
    @DisplayName("Analitica: un duplicado no rompe el listener")
    void analiticaToleraDuplicado() {
        AtencionEvento evento = Eventos.creada(41, 1, "2026-11-13T15:00:00Z");
        when(metricas.registrarAtencion(any(), anyInt(), anyLong(), anyString())).thenReturn(false);

        analitica.escuchar(registro(evento));

        verify(metricas).registrarAtencion(any(), anyInt(), anyLong(), anyString());
    }

    @Test
    @DisplayName("Mensaje sin contenido (null): invalido, va a la DLT sin reintentos")
    void mensajeVacio() {
        assertThatThrownBy(() -> historialListener.escuchar(registro(null)))
                .isInstanceOf(EventoInvalidoException.class);
        verifyNoInteractions(historial);
    }

    @Test
    @DisplayName("Historial: un evento valido se guarda con su particion y offset")
    void historialDelega() {
        AtencionEvento evento = Eventos.creada(41, 1, "2026-11-13T15:00:00Z");
        when(historial.registrar(evento, 2, 7L)).thenReturn(true);

        historialListener.escuchar(registro(evento));

        verify(historial).registrar(evento, 2, 7L);
    }

    @Test
    @DisplayName("Boxes: un evento valido actualiza el estado del box")
    void boxesDelega() {
        BoxEstadoEvento evento = Eventos.box(3, 2, true);
        when(metricas.registrarBox(evento)).thenReturn(true);

        boxes.escuchar(new ConsumerRecord<>("vidasalud.boxes.estado", 0, 0L, "3", evento));

        verify(metricas).registrarBox(evento);
    }

    @Test
    @DisplayName("Al arrancar se rebobina cada particion una sola vez")
    void rebobinaSoloLaPrimeraVez() {
        TopicPartition p0 = new TopicPartition(TOPICO, 0);
        TopicPartition p1 = new TopicPartition(TOPICO, 1);

        analitica.onPartitionsAssigned(Map.of(p0, 10L), callback);
        analitica.onPartitionsAssigned(Map.of(p0, 10L, p1, 4L), callback);

        verify(callback).seekToBeginning(List.of(p0));
        verify(callback).seekToBeginning(List.of(p1));
        verify(callback, times(2)).seekToBeginning(any());
    }

    @Test
    @DisplayName("Si la reconstruccion esta desactivada, no se rebobina")
    void sinRebobinado() {
        AtencionesAnaliticaListener sinReconstruir = new AtencionesAnaliticaListener(validador, metricas, false);

        sinReconstruir.onPartitionsAssigned(Map.of(new TopicPartition(TOPICO, 0), 0L), callback);

        verify(callback, never()).seekToBeginning(any());
    }
}
