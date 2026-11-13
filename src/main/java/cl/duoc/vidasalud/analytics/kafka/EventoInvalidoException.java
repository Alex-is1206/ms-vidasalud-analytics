package cl.duoc.vidasalud.analytics.kafka;

/**
 * El evento llego bien formado como JSON, pero su contenido no cumple el contrato
 * (faltan campos, fecha ilegible, tipo y estados incoherentes).
 *
 * Reintentarlo no sirve: el mismo mensaje fallaria igual las veces que se lea.
 * Por eso KafkaConsumidorConfig la marca como NO reintentable y el mensaje va
 * directo a la DLT, sin bloquear la particion.
 */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
