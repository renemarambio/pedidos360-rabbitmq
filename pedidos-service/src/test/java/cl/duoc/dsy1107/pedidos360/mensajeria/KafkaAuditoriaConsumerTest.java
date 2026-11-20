package cl.duoc.dsy1107.pedidos360.mensajeria;

import cl.duoc.dsy1107.pedidos360.dominio.auditoria.KafkaAuditoriaConsumer;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.TipoEventoPedido;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.KafkaPedidoEventPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Pruebas unitarias para el consumidor de auditoría Kafka.
 */
class KafkaAuditoriaConsumerTest {

    private KafkaPedidoEventPublisher kafkaPublisher;
    private KafkaAuditoriaConsumer consumer;

    @BeforeEach
    void setUp() {
        kafkaPublisher = mock(KafkaPedidoEventPublisher.class);
        consumer = new KafkaAuditoriaConsumer(kafkaPublisher);
    }

    @Test
    void procesarEventoPedido_registraYPublicaEnTimeline() {
        PedidoEventoDTO evento = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_CREATED.getCodigo(),
                7001L,
                Map.of("cliente", "cliente7001@duoc.cl"),
                "trace-123"
        );

        consumer.procesarEventoPedido(evento, "7001", 1, 42L, System.currentTimeMillis());

        verify(kafkaPublisher).publicarAuditTimeline(eq("7001"), any());
    }

    @Test
    void procesarEventoConSimulacionError_lanzaExcepcionParaActivarDlt() {
        PedidoEventoDTO eventoConError = PedidoEventoDTO.crear(
                "OrderAuditErrorSimulation",
                7002L,
                Map.of("simularErrorAuditoria", true),
                "trace-err"
        );

        assertThrows(RuntimeException.class, () ->
                consumer.procesarEventoPedido(eventoConError, "7002", 0, 10L, System.currentTimeMillis())
        );
    }

    @Test
    void procesarDlt_registraMensajeFallidoSinLanzarExcepcion() {
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("orders.events.DLT", 0, 15L, "7003", "payload");
        consumer.procesarDlt(record, 0, 15L);
    }
}
