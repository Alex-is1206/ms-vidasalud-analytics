package cl.duoc.vidasalud.analytics.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Nombres y conexion de Kafka leidos desde application.properties (prefijo vidasalud.kafka).
 *
 * Los cuatro primeros campos son los mismos que en appointments y catalog. Este
 * microservicio agrega los grupos de consumidores y la politica de reintentos, porque
 * es el unico que consume.
 *
 * @param bootstrapServers     brokers a los que se conecta el cliente
 * @param clientId             identificador del cliente, visible en los brokers y en Kafka UI
 * @param topicoAtenciones     topico con el ciclo de vida de las atenciones
 * @param topicoBoxes          topico compactado con el ultimo estado de cada box
 * @param grupoAnalitica       grupo que mantiene las metricas en memoria
 * @param grupoHistorial       grupo que guarda la linea de tiempo de cada atencion
 * @param reintentos           reintentos ante un error pasajero antes de enviar a la DLT
 * @param intervaloReintentoMs espera entre reintentos, en milisegundos
 * @param sufijoDlt            sufijo del topico de errores (.DLT), igual al que crea kafka-admin
 */
@ConfigurationProperties("vidasalud.kafka")
public record NombresKafka(
        String bootstrapServers,
        String clientId,
        String topicoAtenciones,
        String topicoBoxes,
        String grupoAnalitica,
        String grupoHistorial,
        long reintentos,
        long intervaloReintentoMs,
        String sufijoDlt
) {
}
