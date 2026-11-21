package cl.duoc.dsy1107.pedidos360.dominio.metricas;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.TipoEventoPedido;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MetricasPedidosConsumerTest {

    private MetricasPedidosConsumer consumer;
    private MetricasKafkaController controller;

    @BeforeEach
    void setUp() {
        consumer = new MetricasPedidosConsumer();
        consumer.limpiarParaPruebas();
        controller = new MetricasKafkaController(consumer);
    }

    @Test
    void procesarEventoMetricas_incrementaContadoresPorTipoYParticion() {
        PedidoEventoDTO evento1 = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_CREATED.getCodigo(),
                1001L,
                Map.of("pedidoId", 1001L),
                "evt-1001"
        );
        PedidoEventoDTO evento2 = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_DELIVERED.getCodigo(),
                1002L,
                Map.of("pedidoId", 1002L),
                "evt-1002"
        );
        PedidoEventoDTO evento3 = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_CREATED.getCodigo(),
                1003L,
                Map.of("pedidoId", 1003L),
                "evt-1003"
        );

        consumer.procesarEventoMetricas(evento1, "1001", "orders.events", 0, 100L);
        consumer.procesarEventoMetricas(evento2, "1002", "orders.events", 1, 101L);
        consumer.procesarEventoMetricas(evento3, "1003", "orders.events", 0, 102L);

        Map<String, Object> metricas = consumer.obtenerMetricas();
        assertEquals(3L, metricas.get("totalEventos"));

        @SuppressWarnings("unchecked")
        Map<String, Long> porTipo = (Map<String, Long>) metricas.get("eventosPorTipo");
        assertEquals(2L, porTipo.get(TipoEventoPedido.ORDER_CREATED.getCodigo()));
        assertEquals(1L, porTipo.get(TipoEventoPedido.ORDER_DELIVERED.getCodigo()));

        @SuppressWarnings("unchecked")
        Map<String, Long> porParticion = (Map<String, Long>) metricas.get("eventosPorParticion");
        assertEquals(2L, porParticion.get("particion-0"));
        assertEquals(1L, porParticion.get("particion-1"));

        @SuppressWarnings("unchecked")
        Map<String, Long> porHilo = (Map<String, Long>) metricas.get("eventosPorHilo");
        assertFalse(porHilo.isEmpty());
        assertTrue(porHilo.containsKey(Thread.currentThread().getName()));

        // Validar endpoint del controlador
        ResponseEntity<Map<String, Object>> respuesta = controller.obtenerMetricas();
        assertTrue(respuesta.getStatusCode().is2xxSuccessful());
        assertNotNull(respuesta.getBody());
        assertEquals(3L, respuesta.getBody().get("totalEventos"));
    }
}
