package cl.duoc.vidasalud.analytics.kafka.evento;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * Evento del topico compactado vidasalud.boxes.estado, visto desde el consumidor.
 *
 * Mismo contrato que publica ms-vidasalud-catalog. Cada mensaje es una foto completa
 * del box, asi que el ultimo mensaje de cada clave (boxId) basta para conocer su
 * estado actual: por eso el topico es compactado.
 */
public record BoxEstadoEvento(
        @NotBlank String eventId,
        @NotBlank String ocurridoEn,
        @NotNull Long boxId,
        @NotBlank String nombre,
        @NotNull @PositiveOrZero Integer capacidad,
        @NotNull Boolean activo,
        @NotNull AccionBox accion,
        @NotBlank String origen,
        int version
) {
}
