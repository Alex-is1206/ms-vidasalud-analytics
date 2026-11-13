package cl.duoc.vidasalud.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import cl.duoc.vidasalud.analytics.kafka.config.NombresKafka;

/**
 * Microservicio de analitica de VidaSalud.
 *
 * Consume los eventos de Kafka que publican appointments (ciclo de vida de las
 * atenciones) y catalog (estado de los boxes) para mantener metricas en tiempo real
 * y el historial de cada atencion. No publica eventos de negocio: solo los lee.
 */
@SpringBootApplication
@EnableConfigurationProperties(NombresKafka.class)
public class MsVidasaludAnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsVidasaludAnalyticsApplication.class, args);
    }
}
