package cl.duoc.vidasalud.analytics;

import java.util.UUID;

import cl.duoc.vidasalud.analytics.kafka.evento.AccionBox;
import cl.duoc.vidasalud.analytics.kafka.evento.AtencionEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.BoxEstadoEvento;
import cl.duoc.vidasalud.analytics.kafka.evento.TipoEventoAtencion;

/** Eventos de ejemplo para las pruebas. */
public final class Eventos {

    private Eventos() {
    }

    public static AtencionEvento creada(long atencionId, long prestacionId, String ocurridoEn) {
        return new AtencionEvento(UUID.randomUUID().toString(), TipoEventoAtencion.ATENCION_CREADA, ocurridoEn,
                atencionId, null, "SOLICITADA", prestacionId, 1L, "2026-11-20T09:00", "ms-vidasalud-appointments", 1);
    }

    public static AtencionEvento cambio(long atencionId, String anterior, String nuevo, String ocurridoEn) {
        return new AtencionEvento(UUID.randomUUID().toString(), TipoEventoAtencion.ESTADO_CAMBIADO, ocurridoEn,
                atencionId, anterior, nuevo, 1L, 1L, "2026-11-20T09:00", "ms-vidasalud-appointments", 1);
    }

    public static BoxEstadoEvento box(long boxId, int capacidad, boolean activo) {
        return new BoxEstadoEvento(UUID.randomUUID().toString(), "2026-11-13T15:00:00Z", boxId, "Box " + boxId,
                capacidad, activo, AccionBox.CREADO, "ms-vidasalud-catalog", 1);
    }
}
