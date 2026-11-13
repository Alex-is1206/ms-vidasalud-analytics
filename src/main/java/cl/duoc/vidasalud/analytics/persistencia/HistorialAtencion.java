package cl.duoc.vidasalud.analytics.persistencia;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

/**
 * Una fila por evento de Kafka aplicado al historial de una atencion.
 *
 * - EVENT_ID es UNIQUE: aunque el mismo evento llegue dos veces (relectura del topico,
 *   reintento, reinicio antes del commit del offset), Oracle solo guarda uno. Es la
 *   idempotencia que sobrevive a los reinicios.
 * - PARTICION y OFFSET_KAFKA dejan la traza exacta del mensaje de origen.
 *   (OFFSET es palabra reservada en Oracle, por eso OFFSET_KAFKA).
 * - El indice por ATENCION_ID acelera la consulta de la linea de tiempo.
 */
@Entity
@Table(name = "HISTORIAL_ATENCION",
        uniqueConstraints = @UniqueConstraint(name = "UK_HISTORIAL_EVENT_ID", columnNames = "EVENT_ID"),
        indexes = @Index(name = "IX_HISTORIAL_ATENCION_ID", columnList = "ATENCION_ID"))
public class HistorialAtencion {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "historial_seq")
    @SequenceGenerator(name = "historial_seq", sequenceName = "HISTORIAL_ATENCION_SEQ", allocationSize = 1)
    private Long id;

    @Column(name = "EVENT_ID", nullable = false, length = 64)
    private String eventId;

    @Column(name = "ATENCION_ID", nullable = false)
    private Long atencionId;

    @Column(name = "TIPO", nullable = false, length = 30)
    private String tipo;

    @Column(name = "ESTADO_ANTERIOR", length = 20)
    private String estadoAnterior;

    @Column(name = "ESTADO_NUEVO", nullable = false, length = 20)
    private String estadoNuevo;

    @Column(name = "PRESTACION_ID")
    private Long prestacionId;

    @Column(name = "BOX_ID")
    private Long boxId;

    @Column(name = "OCURRIDO_EN", nullable = false)
    private Instant ocurridoEn;

    @Column(name = "PROCESADO_EN", nullable = false)
    private Instant procesadoEn;

    @Column(name = "PARTICION", nullable = false)
    private Integer particion;

    @Column(name = "OFFSET_KAFKA", nullable = false)
    private Long offsetKafka;

    /** Requerido por JPA. */
    protected HistorialAtencion() {
    }

    public HistorialAtencion(String eventId, Long atencionId, String tipo, String estadoAnterior,
                             String estadoNuevo, Long prestacionId, Long boxId, Instant ocurridoEn,
                             Instant procesadoEn, Integer particion, Long offsetKafka) {
        this.eventId = eventId;
        this.atencionId = atencionId;
        this.tipo = tipo;
        this.estadoAnterior = estadoAnterior;
        this.estadoNuevo = estadoNuevo;
        this.prestacionId = prestacionId;
        this.boxId = boxId;
        this.ocurridoEn = ocurridoEn;
        this.procesadoEn = procesadoEn;
        this.particion = particion;
        this.offsetKafka = offsetKafka;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public Long getAtencionId() { return atencionId; }
    public String getTipo() { return tipo; }
    public String getEstadoAnterior() { return estadoAnterior; }
    public String getEstadoNuevo() { return estadoNuevo; }
    public Long getPrestacionId() { return prestacionId; }
    public Long getBoxId() { return boxId; }
    public Instant getOcurridoEn() { return ocurridoEn; }
    public Instant getProcesadoEn() { return procesadoEn; }
    public Integer getParticion() { return particion; }
    public Long getOffsetKafka() { return offsetKafka; }
}
