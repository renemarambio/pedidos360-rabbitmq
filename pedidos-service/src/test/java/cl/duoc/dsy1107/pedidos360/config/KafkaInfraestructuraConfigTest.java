package cl.duoc.dsy1107.pedidos360.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Pruebas unitarias para la configuración programática de infraestructura Kafka.
 */
class KafkaInfraestructuraConfigTest {

    private KafkaInfraestructuraConfig config;

    @BeforeEach
    void setUp() {
        config = new KafkaInfraestructuraConfig();
        ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");
        ReflectionTestUtils.setField(config, "topicoOrdersEvents", "orders.events");
        ReflectionTestUtils.setField(config, "topicoAuditTimeline", "audit.timeline");
        ReflectionTestUtils.setField(config, "topicoOrdersEventsDlt", "orders.events.DLT");
        ReflectionTestUtils.setField(config, "particiones", 3);
        ReflectionTestUtils.setField(config, "replicas", (short) 1);
    }

    @Test
    void ordersEventsTopic_declaraTresParticionesYReplicacionUno() {
        NewTopic topic = config.ordersEventsTopic();
        assertNotNull(topic);
        assertEquals("orders.events", topic.name());
        assertEquals(3, topic.numPartitions());
        assertEquals((short) 1, topic.replicationFactor());
    }

    @Test
    void auditTimelineTopic_declaraTresParticionesYReplicacionUno() {
        NewTopic topic = config.auditTimelineTopic();
        assertNotNull(topic);
        assertEquals("audit.timeline", topic.name());
        assertEquals(3, topic.numPartitions());
        assertEquals((short) 1, topic.replicationFactor());
    }

    @Test
    void ordersEventsDltTopic_declaraTopicoDlt() {
        NewTopic topic = config.ordersEventsDltTopic();
        assertNotNull(topic);
        assertEquals("orders.events.DLT", topic.name());
        assertEquals(3, topic.numPartitions());
        assertEquals((short) 1, topic.replicationFactor());
    }

    @Test
    @SuppressWarnings("unchecked")
    void kafkaErrorHandler_instanciaManejadorConReintentosYDlt() {
        KafkaOperations<Object, Object> ops = mock(KafkaOperations.class);
        DefaultErrorHandler handler = config.kafkaErrorHandler(ops);
        assertNotNull(handler);
    }
}
