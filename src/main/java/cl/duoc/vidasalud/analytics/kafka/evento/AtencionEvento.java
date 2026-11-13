package cl.duoc.vidasalud.analytics.kafka.evento;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * Evento del topico vidasalud.atenciones.eventos, visto desde el consumidor.
 *
 * Es el mismo contrato que publica ms-vidasalud-appointments: mismos campos y mismos
 * nombres. Cada microservicio tiene su propia copia del record (en su propio paquete)
 * y por eso los mensajes viajan sin cabeceras de tipo: el consumidor siempre
 * deserializa a esta clase.
 *
 * Las anotaciones no se activan solas al deserializar: las revisa ValidadorEventos
 * al entrar a cada listener.
 */
public record AtencionEvento(
        @NotBlank String eventId,
        @NotNull TipoEventoAtencion tipo,
        @NotBlank String ocurridoEn,
        @NotNull @Positive Long atencionId,
        String estadoAnterior,
        @NotBlank String estadoNuevo,
        @NotNull Long prestacionId,
        Long boxId,
        String fechaHoraAtencion,
        @NotBlank String origen,
        int version
) {
}
