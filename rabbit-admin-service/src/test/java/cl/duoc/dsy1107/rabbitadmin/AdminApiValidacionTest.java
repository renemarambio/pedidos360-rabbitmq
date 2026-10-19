package cl.duoc.dsy1107.rabbitadmin;

import cl.duoc.dsy1107.rabbitadmin.api.BindingController;
import cl.duoc.dsy1107.rabbitadmin.api.ExchangeController;
import cl.duoc.dsy1107.rabbitadmin.api.PoliticaController;
import cl.duoc.dsy1107.rabbitadmin.api.QueueController;
import cl.duoc.dsy1107.rabbitadmin.dto.ColaInfo;
import cl.duoc.dsy1107.rabbitadmin.exception.ConflictoException;
import cl.duoc.dsy1107.rabbitadmin.exception.OperacionNoPermitidaException;
import cl.duoc.dsy1107.rabbitadmin.exception.RecursoNoEncontradoException;
import cl.duoc.dsy1107.rabbitadmin.service.RabbitAdminService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Pruebas de la API del administrador: verbos, códigos HTTP y validación de entrada.
 * El servicio se simula, por lo que no se necesita RabbitMQ.
 */
@WebMvcTest({QueueController.class, ExchangeController.class, BindingController.class, PoliticaController.class})
class AdminApiValidacionTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private RabbitAdminService adminService;

    // ---------- Colas ----------

    @Test
    void crearColaValida_responde201ConLocation() throws Exception {
        when(adminService.crearCola(any())).thenReturn(
                new ColaInfo("demo.pagos.queue", 0, 0, 0, 0, true, null, null, Map.of()));

        mvc.perform(post("/api/admin/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"demo.pagos.queue\",\"ttlMs\":60000,\"maxLength\":500}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/admin/queues/demo.pagos.queue"))
                .andExpect(jsonPath("$.nombre").value("demo.pagos.queue"));
    }

    @Test
    void crearColaConNombreVacio_responde400() throws Exception {
        mvc.perform(post("/api/admin/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.nombre").exists());

        verify(adminService, never()).crearCola(any());
    }

    @Test
    void crearColaConCaracteresInvalidos_responde400() throws Exception {
        mvc.perform(post("/api/admin/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"mi cola!\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.nombre").exists());
    }

    @Test
    void crearColaConTtlNegativo_responde400() throws Exception {
        mvc.perform(post("/api/admin/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"demo.queue\",\"ttlMs\":-5}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.ttlMs").exists());
    }

    @Test
    void crearColaConRoutingKeyDeadLetterSinExchange_responde400() throws Exception {
        mvc.perform(post("/api/admin/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"demo.queue\",\"deadLetterRoutingKey\":\"demo.failed\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.deadLetterConsistente").exists());
    }

    @Test
    void crearColaExistente_responde409() throws Exception {
        when(adminService.crearCola(any())).thenThrow(new ConflictoException("La cola 'demo.queue' ya existe"));

        mvc.perform(post("/api/admin/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"demo.queue\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.mensaje").value("La cola 'demo.queue' ya existe"));
    }

    @Test
    void eliminarColaInexistente_responde404() throws Exception {
        doThrow(new RecursoNoEncontradoException("La cola 'no.existe' no existe"))
                .when(adminService).eliminarCola(eq("no.existe"), anyBoolean(), anyBoolean());

        mvc.perform(delete("/api/admin/queues/no.existe"))
                .andExpect(status().isNotFound());
    }

    @Test
    void eliminarColaConNombreInvalido_responde400() throws Exception {
        mvc.perform(delete("/api/admin/queues/ab"))
                .andExpect(status().isBadRequest());

        verify(adminService, never()).eliminarCola(any(), anyBoolean(), anyBoolean());
    }

    @Test
    void eliminarColaExistente_responde204() throws Exception {
        mvc.perform(delete("/api/admin/queues/demo.queue").param("soloSiVacia", "true"))
                .andExpect(status().isNoContent());

        verify(adminService).eliminarCola("demo.queue", true, false);
    }

    // ---------- Exchanges ----------

    @Test
    void crearExchangeConTipoInvalido_responde400() throws Exception {
        mvc.perform(post("/api/admin/exchanges").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"demo.exchange\",\"tipo\":\"SUPER\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void crearExchangeSinTipo_responde400() throws Exception {
        mvc.perform(post("/api/admin/exchanges").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"demo.exchange\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.tipo").exists());
    }

    @Test
    void eliminarExchangeReservado_responde403() throws Exception {
        doThrow(new OperacionNoPermitidaException("El prefijo 'amq.' está reservado"))
                .when(adminService).eliminarExchange("amq.direct");

        mvc.perform(delete("/api/admin/exchanges/amq.direct"))
                .andExpect(status().isForbidden());
    }

    // ---------- Bindings ----------

    @Test
    void crearBindingSinCola_responde400() throws Exception {
        mvc.perform(post("/api/admin/bindings").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"exchange\":\"demo.exchange\",\"routingKey\":\"pagos.*\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.cola").exists());
    }

    // ---------- Políticas ----------

    @Test
    void politicaSinTtlNiMaxLength_responde400() throws Exception {
        mvc.perform(put("/api/admin/policies/retencion-demo").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"patron\":\"^demo\\\\.\"}"))
                .andExpect(status().isBadRequest());

        verify(adminService, never()).aplicarPolitica(any(), any());
    }
}
