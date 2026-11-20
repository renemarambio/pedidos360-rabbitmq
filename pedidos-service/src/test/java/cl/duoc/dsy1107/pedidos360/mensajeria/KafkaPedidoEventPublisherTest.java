package cl.duoc.dsy1107.pedidos360.mensajeria;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.TipoEventoPedido;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.KafkaPedidoEventPublisher;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pruebas unitarias para el publicador de eventos Kafka.
 * Valida que la clave (Key) sea el pedidoId para garantizar el orden de los eventos en la misma partición.
 */
class KafkaPedidoEventPublisherTest {

    private KafkaTemplate<String, Object> kafkaTemplate;
    private KafkaPedidoEventPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new KafkaPedidoEventPublisher(kafkaTemplate, "orders.events", "audit.timeline");
    }

    @Test
    void publicarEventoPedido_utilizaPedidoIdComoClaveDeParticion() {
        Long pedidoId = 5001L;
        PedidoEventoDTO evento = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_CREATED.getCodigo(),
                pedidoId,
                Map.of("total", 19990),
                "trace-abc"
        );

        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders.events", 1), 0L, 0, 0L, 0, 0);
        future.complete(new SendResult<>(null, metadata));

        when(kafkaTemplate.send(eq("orders.events"), eq("5001"), any(PedidoEventoDTO.class)))
                .thenReturn(future);

        CompletableFuture<SendResult<String, Object>> resultado = publisher.publicarEventoPedido(evento);

        assertNotNull(resultado);
        verify(kafkaTemplate).send(eq("orders.events"), eq("5001"), eq(evento));
        assertEquals(pedidoId, evento.obtenerPedidoId());
    }

    @Test
    void publicarAuditTimeline_enviaMensajeAlTopicoDeAuditoria() {
        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(eq("audit.timeline"), eq("5002"), any()))
                .thenReturn(future);

        publisher.publicarAuditTimeline("5002", Map.of("evento", "OrderAccepted"));

        verify(kafkaTemplate).send(eq("audit.timeline"), eq("5002"), any());
    }
}
