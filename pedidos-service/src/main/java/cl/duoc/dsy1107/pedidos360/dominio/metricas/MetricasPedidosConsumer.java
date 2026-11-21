package cl.duoc.dsy1107.pedidos360.dominio.metricas;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Segundo Consumer Group independiente ('pedidos360-metricas-group') sobre 'orders.events'.
 *
 * Configurado con concurrency = "3" para asignar un hilo dedicado a cada una de las 3 particiones,
 * calculando métricas de eventos en tiempo real y evidenciando el paralelismo de consumo.
 */
@Component
public class MetricasPedidosConsumer {

    private static final Logger log = LoggerFactory.getLogger(MetricasPedidosConsumer.class);

    private final AtomicLong totalEventosProcesados = new AtomicLong(0);
    private final Map<String, AtomicLong> eventosPorTipo = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> eventosPorParticion = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> eventosPorHilo = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "${pedidos360.kafka.topicos.orders-events:orders.events}",
            groupId = "pedidos360-metricas-group",
            concurrency = "3",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void procesarEventoMetricas(
            @Payload PedidoEventoDTO evento,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topico,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer particion,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset
    ) {
        String nombreHilo = Thread.currentThread().getName();
        String tipoEvento = (evento != null && evento.tipoEvento() != null) ? evento.tipoEvento() : "DESCONOCIDO";
        int particionEfectiva = (particion != null) ? particion : -1;

        totalEventosProcesados.incrementAndGet();
        eventosPorTipo.computeIfAbsent(tipoEvento, k -> new AtomicLong(0)).incrementAndGet();
        if (particion != null) {
            eventosPorParticion.computeIfAbsent(particion, k -> new AtomicLong(0)).incrementAndGet();
        }
        eventosPorHilo.computeIfAbsent(nombreHilo, k -> new AtomicLong(0)).incrementAndGet();

        log.info("""
                ------------------ [KAFKA MÉTRICAS CONSUMER] ------------------
                Consumer Group:    pedidos360-metricas-group
                Hilo Ejecución:    {}
                Tópico:            {}
                Partición:         {}
                Offset:            {}
                Clave (Key):       {}
                Tipo Evento:       {}
                Total Acumulado:   {}
                ---------------------------------------------------------------""",
                nombreHilo,
                topico != null ? topico : "orders.events",
                particionEfectiva,
                offset,
                key,
                tipoEvento,
                totalEventosProcesados.get()
        );
    }

    /**
     * Retorna una instantánea con todas las métricas acumuladas.
     */
    public Map<String, Object> obtenerMetricas() {
        Map<String, Object> metricas = new LinkedHashMap<>();
        metricas.put("consumerGroup", "pedidos360-metricas-group");
        metricas.put("topico", "orders.events");
        metricas.put("concurrency", 3);
        metricas.put("totalEventos", totalEventosProcesados.get());

        Map<String, Long> porTipo = eventosPorTipo.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(), (v1, v2) -> v1, LinkedHashMap::new));
        metricas.put("eventosPorTipo", porTipo);

        Map<String, Long> porParticion = eventosPorParticion.entrySet().stream()
                .collect(Collectors.toMap(e -> "particion-" + e.getKey(), e -> e.getValue().get(), (v1, v2) -> v1, LinkedHashMap::new));
        metricas.put("eventosPorParticion", porParticion);

        Map<String, Long> porHilo = eventosPorHilo.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(), (v1, v2) -> v1, LinkedHashMap::new));
        metricas.put("eventosPorHilo", porHilo);

        metricas.put("timestamp", Instant.now().toString());
        return metricas;
    }

    public void limpiarParaPruebas() {
        totalEventosProcesados.set(0);
        eventosPorTipo.clear();
        eventosPorParticion.clear();
        eventosPorHilo.clear();
    }
}
