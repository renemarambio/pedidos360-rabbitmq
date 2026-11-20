package cl.duoc.dsy1107.pedidos360;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Prueba de punta a punta contra un RabbitMQ real (localhost:5672).
 * Si RabbitMQ no está levantado, la prueba se omite en lugar de fallar.
 */
@SpringBootTest
@AutoConfigureMockMvc
class Pedidos360IntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @BeforeAll
    static void requiereRabbitMq() {
        boolean disponible;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 5672), 500);
            disponible = true;
        } catch (Exception e) {
            disponible = false;
        }
        assumeTrue(disponible, "RabbitMQ no está disponible en localhost:5672; se omite la prueba de integración");
    }

    @Test
    void aceptarPedido_publicaEnLosTresDominios() throws Exception {
        mockMvc.perform(post("/api/lab/pedidos/aceptar").contentType(MediaType.APPLICATION_JSON).content("""
                        {"pedidoId":1001,"clienteEmail":"cliente1@duoc.cl","total":25000}"""))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.publicado").value(true))
                .andExpect(jsonPath("$.pedidoId").value(1001));
    }

    @Test
    void aceptarPedidoConFalloEnCocina_quedaMarcado() throws Exception {
        mockMvc.perform(post("/api/lab/pedidos/aceptar").contentType(MediaType.APPLICATION_JSON).content("""
                        {"pedidoId":1002,"clienteEmail":"cliente2@duoc.cl","total":18500,"falloCocina":true}"""))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.falloCocina").value(true));
    }

    @Test
    void publicarEventoTopic() throws Exception {
        mockMvc.perform(post("/api/lab/eventos/topic").param("routingKey", "pedidos.pagos.confirmado")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                        {"evento":"PEDIDO_PAGADO","pedidoId":1003}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.routingKey").value("pedidos.pagos.confirmado"));
    }
}
