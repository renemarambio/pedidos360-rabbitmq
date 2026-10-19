package cl.duoc.dsy1107.pedidos360.pedidos.api;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.CrearPedidoRequest;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.PedidoSolicitud;
import cl.duoc.dsy1107.pedidos360.pedidos.service.PedidoService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * API REST de pedidos. Solo recibe, valida y responde;
 * la lógica está en {@link PedidoService} y la mensajería en el publicador.
 */
@Validated
@RestController
@RequestMapping("/api")
public class PedidoController {

    private final PedidoService pedidoService;

    public PedidoController(PedidoService pedidoService) {
        this.pedidoService = pedidoService;
    }

    /** Pedido desde el catálogo web. */
    @PostMapping("/pedidos")
    public ResponseEntity<Map<String, Object>> crearPedido(@Valid @RequestBody CrearPedidoRequest request) {
        PedidoEvento evento = pedidoService.crearPedido(request);

        Map<String, Object> respuesta = respuestaBase(evento, "Pedido recibido y distribuido a los dominios");
        respuesta.put("idPedido", evento.metadatos().get("idPedido"));
        respuesta.put("productoId", request.productoId());
        respuesta.put("nombreProducto", request.nombreProducto());
        respuesta.put("precio", request.precio());
        respuesta.put("cantidad", request.cantidad());
        respuesta.put("fecha", evento.metadatos().get("timestamp"));
        return ResponseEntity.status(HttpStatus.CREATED).body(respuesta);
    }

    /** Pedido de laboratorio: permite activar fallos controlados por dominio. */
    @PostMapping({"/lab/pedidos/aceptar", "/eventos/pedido-aceptado"})
    public ResponseEntity<Map<String, Object>> aceptarPedido(@Valid @RequestBody PedidoSolicitud solicitud) {
        PedidoEvento evento = pedidoService.aceptarPedido(solicitud);

        Map<String, Object> respuesta = respuestaBase(evento, "Pedido aceptado y distribuido a los dominios");
        respuesta.put("clienteEmail", evento.clienteEmail());
        respuesta.put("falloNotificaciones", evento.falloNotificaciones());
        respuesta.put("falloCocina", evento.falloCocina());
        respuesta.put("falloDocumentos", evento.falloDocumentos());
        respuesta.put("metadatos", evento.metadatos());
        return ResponseEntity.accepted().body(respuesta);
    }

    /** Evento de monitoreo hacia el topic exchange (ej. routingKey=pedidos.pagos.confirmado). */
    @PostMapping("/lab/eventos/topic")
    public ResponseEntity<Map<String, Object>> publicarEventoTopic(
            @RequestParam(required = false)
            @Pattern(regexp = "^[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)*$",
                    message = "debe ser palabras separadas por punto, sin * ni # (ej. pedidos.pagos.confirmado)")
            String routingKey,
            @RequestBody(required = false) Map<String, Object> payload) {

        String clave = pedidoService.publicarEventoMonitoreo(routingKey, payload);

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("publicado", true);
        respuesta.put("mensaje", "Evento enviado al topic exchange de monitoreo");
        respuesta.put("routingKey", clave);
        return ResponseEntity.ok(respuesta);
    }

    /** Estado del servicio (lo usa el catálogo web). */
    @GetMapping("/status")
    public Map<String, Object> estado() {
        Map<String, Object> estado = new LinkedHashMap<>();
        estado.put("status", "UP");
        estado.put("servicio", "pedidos-service");
        estado.put("broker", "RabbitMQ (clúster de 2 nodos)");
        estado.put("timestamp", LocalDateTime.now().toString());
        return estado;
    }

    private Map<String, Object> respuestaBase(PedidoEvento evento, String mensaje) {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("publicado", true);
        respuesta.put("mensaje", mensaje);
        respuesta.put("pedidoId", evento.pedidoId());
        respuesta.put("eventoId", evento.eventoId());
        respuesta.put("total", evento.total());
        return respuesta;
    }
}
