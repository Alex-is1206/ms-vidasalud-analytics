package cl.duoc.vidasalud.analytics.kafka;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.BoxEstadoEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.TipoEventoAtencion;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

/**
 * Revisa cada evento antes de que llegue a la logica de negocio.
 *
 * Dos niveles:
 * 1. Las anotaciones del DTO (@NotBlank, @NotNull, @Positive...), con el Validator de Jakarta.
 * 2. Reglas del contrato que una anotacion no puede expresar: la fecha debe ser ISO-8601
 *    y el tipo debe ser coherente con los estados.
 *
 * Si algo falla lanza EventoInvalidoException, que no se reintenta y va a la DLT.
 */
@Component
public class ValidadorEventos {

    private final Validator validator;

    public ValidadorEventos(Validator validator) {
        this.validator = validator;
    }

    public void validar(AtencionEvento evento) {
        revisarAnotaciones(evento);
        revisarFecha(evento.ocurridoEn(), evento.eventId());

        if (evento.tipo() == TipoEventoAtencion.ATENCION_CREADA && evento.estadoAnterior() != null) {
            throw new EventoInvalidoException(
                    "evento " + evento.eventId() + ": ATENCION_CREADA no puede traer estadoAnterior");
        }
        if (evento.tipo() == TipoEventoAtencion.ESTADO_CAMBIADO && evento.estadoAnterior() == null) {
            throw new EventoInvalidoException(
                    "evento " + evento.eventId() + ": ESTADO_CAMBIADO debe traer estadoAnterior");
        }
    }

    public void validar(BoxEstadoEvento evento) {
        revisarAnotaciones(evento);
        revisarFecha(evento.ocurridoEn(), evento.eventId());
    }

    private void revisarAnotaciones(Object evento) {
        if (evento == null) {
            throw new EventoInvalidoException("el mensaje no trae contenido");
        }
        Set<ConstraintViolation<Object>> errores = validator.validate(evento);
        if (!errores.isEmpty()) {
            String detalle = errores.stream()
                    .sorted(Comparator.comparing(v -> v.getPropertyPath().toString()))
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .collect(Collectors.joining(", "));
            throw new EventoInvalidoException("evento con campos invalidos: " + detalle);
        }
    }

    private void revisarFecha(String fecha, String eventId) {
        try {
            Instant.parse(fecha);
        } catch (DateTimeParseException e) {
            throw new EventoInvalidoException(
                    "evento " + eventId + ": ocurridoEn no es una fecha ISO-8601 (" + fecha + ")");
        }
    }
}
