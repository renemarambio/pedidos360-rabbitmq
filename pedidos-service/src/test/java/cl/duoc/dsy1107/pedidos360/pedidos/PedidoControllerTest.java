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
}
