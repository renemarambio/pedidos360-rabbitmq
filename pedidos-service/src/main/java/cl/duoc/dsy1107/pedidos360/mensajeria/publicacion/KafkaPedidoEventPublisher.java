package cl.duoc.dsy1107.pedidos360.mensajeria.publicacion;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Publicador de eventos en Apache Kafka para la plataforma Pedidos360.
 *
 * Utiliza 'pedidoId' como clave del mensaje (Record Key) para garantizar
 * particionamiento afín y preservación estricta del orden cronológico
 * de todos los eventos correspondientes a un mismo pedido.
 */
@Service
public class KafkaPedidoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPedidoEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topicoOrdersEvents;
    private final String topicoAuditTimeline;

    public KafkaPedidoEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${pedidos360.kafka.topicos.orders-events:orders.events}") String topicoOrdersEvents,
            @Value("${pedidos360.kafka.topicos.audit-timeline:audit.timeline}") String topicoAuditTimeline) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicoOrdersEvents = topicoOrdersEvents;
        this.topicoAuditTimeline = topicoAuditTimeline;
    }

    /**
     * Publica un evento de pedido en el tópico 'orders.events'.
     *
     * @param evento Envelope del evento
     * @return CompletableFuture con el resultado del envío
     */
    public CompletableFuture<SendResult<String, Object>> publicarEventoPedido(PedidoEventoDTO evento) {
        Long pedidoId = evento.obtenerPedidoId();
        // La clave (Key) es obligatoriamente el pedidoId para enrutar a la misma partición
        String key = pedidoId != null ? String.valueOf(pedidoId) : evento.eventId();

        log.info("[KAFKA-PRODUCTOR] Enviando evento tipo='{}' con Key='{}' a tópico '{}' (eventId={}, traceId={})",
                evento.tipoEvento(), key, topicoOrdersEvents, evento.eventId(), evento.traceId());

        try {
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topicoOrdersEvents, key, evento);

            future.whenComplete((resultado, ex) -> {
                if (ex != null) {
                    log.error("[KAFKA-PRODUCTOR] Fallo al publicar evento '{}' para pedidoId={}: {}",
                            evento.tipoEvento(), key, ex.getMessage(), ex);
                } else {
                    log.info("[KAFKA-PRODUCTOR] Confirmación Kafka: evento='{}' | key='{}' -> partición={}, offset={}",
                            evento.tipoEvento(),
                            key,
                            resultado.getRecordMetadata().partition(),
                            resultado.getRecordMetadata().offset());
                }
            });

            return future;
        } catch (Exception ex) {
            log.error("[KAFKA-PRODUCTOR] Error sincrónico al enviar evento Kafka: {}", ex.getMessage(), ex);
            return CompletableFuture.failedFuture(ex);
        }
    }

    /**
     * Publica un evento de auditoría en el tópico 'audit.timeline'.
     */
    public CompletableFuture<SendResult<String, Object>> publicarAuditTimeline(String key, Object eventoAuditoria) {
        log.info("[KAFKA-AUDITORIA-PUB] Publicando entrada en '{}' con key='{}'", topicoAuditTimeline, key);
        try {
            return kafkaTemplate.send(topicoAuditTimeline, key, eventoAuditoria);
        } catch (Exception ex) {
            log.error("[KAFKA-AUDITORIA-PUB] Error al registrar en timeline de auditoría: {}", ex.getMessage());
            return CompletableFuture.failedFuture(ex);
        }
    }
}
