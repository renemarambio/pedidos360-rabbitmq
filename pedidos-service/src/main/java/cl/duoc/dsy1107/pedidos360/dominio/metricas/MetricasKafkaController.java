package cl.duoc.dsy1107.pedidos360.dominio.metricas;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Controlador REST para consultar las métricas de consumo en tiempo real de Apache Kafka.
 */
@RestController
@RequestMapping("/api/kafka")
public class MetricasKafkaController {

    private final MetricasPedidosConsumer metricasConsumer;

    public MetricasKafkaController(MetricasPedidosConsumer metricasConsumer) {
        this.metricasConsumer = metricasConsumer;
    }

    /**
     * Retorna los contadores de eventos agrupados por tipo, partición y distribución de hilos.
     */
    @GetMapping("/metricas")
    public ResponseEntity<Map<String, Object>> obtenerMetricas() {
        return ResponseEntity.ok(metricasConsumer.obtenerMetricas());
    }
}
