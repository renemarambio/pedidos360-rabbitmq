package cl.duoc.dsy1107.pedidos360.pedidos;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import cl.duoc.dsy1107.pedidos360.pedidos.api.PedidoController;
import cl.duoc.dsy1107.pedidos360.pedidos.service.PedidoService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import cl.duoc.dsy1107.pedidos360.dominio.auditoria.AuditTimelineConsumer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Pruebas de la API de pedidos y sus validaciones (sin RabbitMQ). */
@WebMvcTest(PedidoController.class)
class PedidoControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PedidoService pedidoService;

    @MockBean
    private AuditTimelineConsumer auditTimelineConsumer;

    @Test
    void crearPedidoValido_responde201() throws Exception {
        when(pedidoService.crearPedido(any())).thenReturn(new PedidoEvento("evt-1", 5521L, "c@pedidos360.cl",
                BigDecimal.valueOf(25980), false, false, false,
                Map.of("idPedido", "PED-5521", "timestamp", "2026-10-16T13:30:00Z")));

        mockMvc.perform(post("/api/pedidos").contentType(MediaType.APPLICATION_JSON).content("""
                        {"idPedido":"PED-5521","productoId":"PROD-101","nombreProducto":"Burger Gourmet 360",
                         "precio":12990,"cantidad":2,"fecha":"2026-10-16T13:30:00Z"}"""))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.publicado").value(true))
                .andExpect(jsonPath("$.idPedido").value("PED-5521"))
                .andExpect(jsonPath("$.total").value(25980));
    }

    @Test
    void crearPedidoSinProductoNiPrecio_responde400ConDetalle() throws Exception {
        mockMvc.perform(post("/api/pedidos").contentType(MediaType.APPLICATION_JSON).content("""
                        {"cantidad":0}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.nombreProducto").exists())
                .andExpect(jsonPath("$.errores.precio").exists())
                .andExpect(jsonPath("$.errores.cantidad").exists());

        verify(pedidoService, never()).crearPedido(any());
    }

    @Test
    void obtenerTimeline_responde200ConHistorial() throws Exception {
        when(auditTimelineConsumer.obtenerTimelinePedido(5521L)).thenReturn(Map.of(
                "pedidoId", 5521L,
                "ultimoEstado", "OrderDelivered",
                "totalEventos", 2,
                "eventos", java.util.List.of(
                        Map.of("tipoEvento", "OrderCreated", "particion", 1),
                        Map.of("tipoEvento", "OrderDelivered", "particion", 1)
                )
        ));

        mockMvc.perform(get("/api/pedidos/5521/timeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.pedidoId").value(5521))
                .andExpect(jsonPath("$.ultimoEstado").value("OrderDelivered"))
                .andExpect(jsonPath("$.totalEventos").value(2))
                .andExpect(jsonPath("$.eventos[0].tipoEvento").value("OrderCreated"));
    }

    @Test
    void obtenerEstado_responde200ConDetalleCluster() throws Exception {
        mockMvc.perform(get("/api/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.servicio").value("pedidos-service"))
                .andExpect(jsonPath("$.broker").value("RabbitMQ (2 nodos) + Apache Kafka (3 brokers + 3 ZooKeeper)"))
                .andExpect(jsonPath("$.streamingEventos").value("Apache Kafka 3 brokers, RF=3, min.insync.replicas=2"))
                .andExpect(jsonPath("$.topicosKafka.eventos").value("orders.events (3 particiones, RF=3)"))
                .andExpect(jsonPath("$.topicosKafka.auditoria").value("audit.timeline (3 particiones, RF=3)"))
                .andExpect(jsonPath("$.topicosKafka.dlt").value("orders.events.DLT (Dead Letter Topic, 3 particiones, RF=3)"))
                .andExpect(jsonPath("$.consumerGroups[0]").value("pedidos360-audit-group"))
                .andExpect(jsonPath("$.consumerGroups[1]").value("pedidos360-metricas-group"))
                .andExpect(jsonPath("$.consumerGroups[2]").value("pedidos360-timeline-group"))
                .andExpect(jsonPath("$.consumerGroups[3]").value("pedidos360-audit-group-dlt"));
    }
}
