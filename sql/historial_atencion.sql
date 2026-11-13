-- Tabla del historial de atenciones de ms-vidasalud-analytics (esquema VIDASALUD, Oracle XE).
--
-- Hibernate la crea sola al arrancar (spring.jpa.hibernate.ddl-auto=update).
-- Este script es la definicion equivalente, para revisarla o crearla a mano.

CREATE SEQUENCE HISTORIAL_ATENCION_SEQ START WITH 1 INCREMENT BY 1;

CREATE TABLE HISTORIAL_ATENCION (
    ID               NUMBER(19)                  NOT NULL,
    EVENT_ID         VARCHAR2(64)                NOT NULL,   -- id del evento de Kafka
    ATENCION_ID      NUMBER(19)                  NOT NULL,
    TIPO             VARCHAR2(30)                NOT NULL,   -- ATENCION_CREADA | ESTADO_CAMBIADO
    ESTADO_ANTERIOR  VARCHAR2(20),
    ESTADO_NUEVO     VARCHAR2(20)                NOT NULL,
    PRESTACION_ID    NUMBER(19),
    BOX_ID           NUMBER(19),
    OCURRIDO_EN      TIMESTAMP(6) WITH TIME ZONE NOT NULL,   -- cuando paso en appointments
    PROCESADO_EN     TIMESTAMP(6) WITH TIME ZONE NOT NULL,   -- cuando lo guardo analytics
    PARTICION        NUMBER(10)                  NOT NULL,   -- traza del mensaje en Kafka
    OFFSET_KAFKA     NUMBER(19)                  NOT NULL,
    CONSTRAINT PK_HISTORIAL_ATENCION PRIMARY KEY (ID),
    -- Idempotencia: un mismo evento no se guarda dos veces
    CONSTRAINT UK_HISTORIAL_EVENT_ID UNIQUE (EVENT_ID)
);

CREATE INDEX IX_HISTORIAL_ATENCION_ID ON HISTORIAL_ATENCION (ATENCION_ID);

-- Consultas utiles para la demo:
-- SELECT ATENCION_ID, TIPO, ESTADO_ANTERIOR, ESTADO_NUEVO, PARTICION, OFFSET_KAFKA
--   FROM HISTORIAL_ATENCION ORDER BY ATENCION_ID, OCURRIDO_EN;
-- SELECT ESTADO_NUEVO, COUNT(*) FROM HISTORIAL_ATENCION GROUP BY ESTADO_NUEVO;
