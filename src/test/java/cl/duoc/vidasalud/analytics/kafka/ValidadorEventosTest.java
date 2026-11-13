package cl.duoc.vidasalud.analytics.kafka;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import cl.duoc.vidasalud.analytics.Eventos;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.TipoEventoAtencion;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;

class ValidadorEventosTest {

    private static ValidatorFactory fabrica;
    private static ValidadorEventos validador;

    @BeforeAll
    static void crear() {
        fabrica = Validation.buildDefaultValidatorFactory();
        validador = new ValidadorEventos(fabrica.getValidator());
    }

    @AfterAll
    static void cerrar() {
        fabrica.close();
    }

    @Test
    @DisplayName("Un evento completo y coherente pasa la validacion")
    void eventoValido() {
        assertThatCode(() -> validador.validar(Eventos.creada(1, 1, "2026-11-13T15:00:00Z")))
                .doesNotThrowAnyException();
        assertThatCode(() -> validador.validar(Eventos.box(1, 2, true)))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Mensaje sin contenido: invalido")
    void eventoNulo() {
        assertThatThrownBy(() -> validador.validar((AtencionEvento) null))
                .isInstanceOf(EventoInvalidoException.class);
    }

    @Test
    @DisplayName("Faltan campos obligatorios: invalido y el mensaje dice cuales")
    void faltanCampos() {
        AtencionEvento incompleto = new AtencionEvento("evt-1", TipoEventoAtencion.ESTADO_CAMBIADO,
                "2026-11-13T15:00:00Z", null, "SOLICITADA", "", 1L, null, null, "x", 1);

        assertThatThrownBy(() -> validador.validar(incompleto))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("atencionId")
                .hasMessageContaining("estadoNuevo");
    }

    @Test
    @DisplayName("Fecha que no es ISO-8601: invalido")
    void fechaIlegible() {
        assertThatThrownBy(() -> validador.validar(Eventos.creada(1, 1, "13/11/2026")))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("ISO-8601");
    }

    @Test
    @DisplayName("Tipo incoherente con los estados: invalido")
    void tipoIncoherente() {
        AtencionEvento cambioSinAnterior = Eventos.cambio(1, null, "CONFIRMADA", "2026-11-13T15:00:00Z");

        assertThatThrownBy(() -> validador.validar(cambioSinAnterior))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("estadoAnterior");
    }
}
