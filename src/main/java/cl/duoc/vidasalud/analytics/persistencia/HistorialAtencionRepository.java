package cl.duoc.vidasalud.analytics.persistencia;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface HistorialAtencionRepository extends JpaRepository<HistorialAtencion, Long> {

    boolean existsByEventId(String eventId);

    /** Linea de tiempo de una atencion; el id desempata eventos del mismo instante. */
    List<HistorialAtencion> findByAtencionIdOrderByOcurridoEnAscIdAsc(Long atencionId);
}
