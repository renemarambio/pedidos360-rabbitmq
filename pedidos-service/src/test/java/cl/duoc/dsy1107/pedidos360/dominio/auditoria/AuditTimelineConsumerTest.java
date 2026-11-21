package cl.duoc.dsy1107.pedidos360.dominio.auditoria;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AuditTimelineConsumerTest {

    private AuditTimelineConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AuditTimelineConsumer();
        consumer.limpiarParaPruebas();
    }

    @Test
    void consumirEventoTimeline_registraUltimoEstadoYHistorial() {
        Map<String, Object> evento1 = new LinkedHashMap<>();
        evento1.put("pedidoId", 8001L);
        evento1.put("tipoEvento", "OrderCreated");
        evento1.put("traceId", "trace-1");

        consumer.consumirEventoTimeline(evento1, "8001", "audit.timeline", 0, 10L, 1700000000000L);

        Map<String, Object> timeline = consumer.obtenerTimelinePedido(8001L);
        assertEquals(8001L, timeline.get("pedidoId"));
        assertEquals("OrderCreated", timeline.get("ultimoEstado"));
        assertEquals(1, timeline.get("totalEventos"));

        // Segundo evento para el mismo pedido (transición a OrderPreparing)
        Map<String, Object> evento2 = new LinkedHashMap<>();
        evento2.put("pedidoId", 8001L);
        evento2.put("tipoEvento", "OrderPreparing");
        evento2.put("traceId", "trace-2");

        consumer.consumirEventoTimeline(evento2, "8001", "audit.timeline", 0, 11L, 1700000010000L);

        Map<String, Object> timelineActualizada = consumer.obtenerTimelinePedido(8001L);
        assertEquals("OrderPreparing", timelineActualizada.get("ultimoEstado"));
        assertEquals(2, timelineActualizada.get("totalEventos"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> eventos = (List<Map<String, Object>>) timelineActualizada.get("eventos");
        assertEquals("OrderCreated", eventos.get(0).get("tipoEvento"));
        assertEquals("OrderPreparing", eventos.get(1).get("tipoEvento"));
        assertEquals(0, eventos.get(0).get("particion"));
        assertEquals(10L, eventos.get(0).get("offset"));
        assertEquals("8001", eventos.get(0).get("key"));
    }

    @Test
    void extraerPedidoIdDesdeClave_cuandoPayloadNoTraePedidoId() {
        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("tipoEvento", "OrderDispatched");

        consumer.consumirEventoTimeline(evento, "8002", "audit.timeline", 2, 50L, 1700000020000L);

        Map<String, Object> timeline = consumer.obtenerTimelinePedido(8002L);
        assertEquals(8002L, timeline.get("pedidoId"));
        assertEquals("OrderDispatched", timeline.get("ultimoEstado"));
        assertEquals(1, timeline.get("totalEventos"));
    }

    @Test
    void pedidoSinEventos_retornaTimelineVacia() {
        Map<String, Object> timeline = consumer.obtenerTimelinePedido(9999L);
        assertEquals(9999L, timeline.get("pedidoId"));
        assertNull(timeline.get("ultimoEstado"));
        assertEquals(0, timeline.get("totalEventos"));
        assertTrue(((List<?>) timeline.get("eventos")).isEmpty());
    }

    @Test
    void consumirEventoTimeline_evitaDuplicadosMismoTipoEnHistorial() {
        Map<String, Object> evento1 = new LinkedHashMap<>();
        evento1.put("pedidoId", 8003L);
        evento1.put("tipoEvento", "OrderPreparing");
        evento1.put("origen", "DOMINIO_COCINA");
        consumer.consumirEventoTimeline(evento1, "8003", "audit.timeline", 0, 20L, 1700000030000L);

        Map<String, Object> evento2 = new LinkedHashMap<>();
        evento2.put("pedidoId", 8003L);
        evento2.put("tipoEvento", "OrderPreparing");
        evento2.put("responsable", "Chef Juan");
        consumer.consumirEventoTimeline(evento2, "8003", "audit.timeline", 0, 21L, 1700000031000L);

        Map<String, Object> timeline = consumer.obtenerTimelinePedido(8003L);
        assertEquals(1, timeline.get("totalEventos"));
        assertEquals("OrderPreparing", timeline.get("ultimoEstado"));
        @SuppressWarnings("unchecked")
        Map<String, Object> ultimo = (Map<String, Object>) timeline.get("ultimoRegistro");
        assertEquals("Chef Juan", ultimo.get("responsable"));
    }
}
