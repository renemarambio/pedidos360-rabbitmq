package cl.duoc.dsy1107.pedidos360.pedidos.api;

import cl.duoc.dsy1107.pedidos360.dominio.auditoria.AuditTimelineConsumer;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.CrearPedidoRequest;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.PedidoSolicitud;
import cl.duoc.dsy1107.pedidos360.pedidos.service.PedidoService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
 * la lógica está en {@link PedidoService} y la mensajería en RabbitMQ y Apache Kafka.
 */
@Validated
@RestController
@RequestMapping("/api")
public class PedidoController {

    private final PedidoService pedidoService;
    private final AuditTimelineConsumer auditTimelineConsumer;

    public PedidoController(PedidoService pedidoService, AuditTimelineConsumer auditTimelineConsumer) {
        this.pedidoService = pedidoService;
        this.auditTimelineConsumer = auditTimelineConsumer;
    }

    /**
     * Devuelve la línea de tiempo de auditoría registrada para un pedido.
     */
    @GetMapping("/pedidos/{id}/timeline")
    public ResponseEntity<Map<String, Object>> obtenerTimeline(@PathVariable("id") Long id) {
        Map<String, Object> timeline = auditTimelineConsumer.obtenerTimelinePedido(id);
        return ResponseEntity.ok(timeline);
    }

    /** Pedido desde el catálogo web. Emite OrderCreated a Kafka y distribuye a RabbitMQ. */
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
        respuesta.put("kafkaEvento", "OrderCreated");
        return ResponseEntity.status(HttpStatus.CREATED).body(respuesta);
    }

    /** Pedido de laboratorio: permite activar fallos controlados por dominio. Emite OrderAccepted a Kafka. */
    @PostMapping({"/lab/pedidos/aceptar", "/eventos/pedido-aceptado"})
    public ResponseEntity<Map<String, Object>> aceptarPedido(@Valid @RequestBody PedidoSolicitud solicitud) {
        PedidoEvento evento = pedidoService.aceptarPedido(solicitud);

        Map<String, Object> respuesta = respuestaBase(evento, "Pedido aceptado y distribuido a los dominios");
        respuesta.put("clienteEmail", evento.clienteEmail());
        respuesta.put("falloNotificaciones", evento.falloNotificaciones());
        respuesta.put("falloCocina", evento.falloCocina());
        respuesta.put("falloDocumentos", evento.falloDocumentos());
        respuesta.put("metadatos", evento.metadatos());
        respuesta.put("kafkaEvento", "OrderAccepted");
        return ResponseEntity.accepted().body(respuesta);
    }

    /**
     * Actualiza el estado de un pedido en Kafka (OrderPreparing, OrderDispatched, OrderDelivered, OrderCancelled).
     * Mantiene estricto orden enviando siempre el pedidoId como clave de partición.
     */
    @PostMapping("/pedidos/{pedidoId}/estado")
    public ResponseEntity<Map<String, Object>> actualizarEstado(
            @PathVariable Long pedidoId,
            @RequestBody Map<String, Object> payload) {
        String estado = (String) payload.getOrDefault("estado", "OrderUpdated");
        PedidoEventoDTO eventoKafka = pedidoService.actualizarEstadoPedido(pedidoId, estado, payload);

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("publicado", true);
        respuesta.put("pedidoId", pedidoId);
        respuesta.put("tipoEvento", eventoKafka.tipoEvento());
        respuesta.put("eventId", eventoKafka.eventId());
        respuesta.put("traceId", eventoKafka.traceId());
        respuesta.put("timestamp", eventoKafka.timestamp());
        respuesta.put("mensaje", "Evento de ciclo de vida publicado en Apache Kafka ('orders.events')");
        return ResponseEntity.ok(respuesta);
    }

    /** Transición explícita a OrderDelivered */
    @PostMapping("/pedidos/{pedidoId}/entregar")
    public ResponseEntity<Map<String, Object>> entregarPedido(
            @PathVariable Long pedidoId,
            @RequestBody(required = false) Map<String, Object> payload) {
        PedidoEventoDTO eventoKafka = pedidoService.entregarPedido(pedidoId, payload);
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("publicado", true);
        respuesta.put("pedidoId", pedidoId);
        respuesta.put("tipoEvento", eventoKafka.tipoEvento());
        respuesta.put("mensaje", "Pedido marcado como entregado (OrderDelivered) en Kafka");
        return ResponseEntity.ok(respuesta);
    }

    /** Transición explícita a OrderCancelled */
    @PostMapping("/pedidos/{pedidoId}/cancelar")
    public ResponseEntity<Map<String, Object>> cancelarPedido(
            @PathVariable Long pedidoId,
            @RequestParam(required = false, defaultValue = "Cancelado por el cliente") String motivo) {
        PedidoEventoDTO eventoKafka = pedidoService.cancelarPedido(pedidoId, motivo);
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("publicado", true);
        respuesta.put("pedidoId", pedidoId);
        respuesta.put("tipoEvento", eventoKafka.tipoEvento());
        respuesta.put("motivo", motivo);
        respuesta.put("mensaje", "Pedido cancelado (OrderCancelled) en Kafka");
        return ResponseEntity.ok(respuesta);
    }

    /** Simulación de fallo para probar reintentos automáticos y desvío hacia Dead Letter Topic (DLT). */
    @PostMapping("/lab/kafka/simular-dlt")
    public ResponseEntity<Map<String, Object>> simularFalloDlt(
            @RequestParam(required = false, defaultValue = "9999") Long pedidoId) {
        PedidoEventoDTO eventoKafka = pedidoService.simularFalloDlt(pedidoId);
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("publicado", true);
        respuesta.put("pedidoId", pedidoId);
        respuesta.put("tipoEvento", eventoKafka.tipoEvento());
        respuesta.put("simularErrorAuditoria", true);
        respuesta.put("mensaje", "Evento con error simulado enviado a Kafka. Verifique en logs: 2 reintentos -> desvío a 'orders.events.DLT'");
        return ResponseEntity.accepted().body(respuesta);
    }

    /** Evento de monitoreo hacia el topic exchange de RabbitMQ (ej. routingKey=pedidos.pagos.confirmado). */
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
        estado.put("broker", "RabbitMQ (2 Nodos) + Apache Kafka (KRaft)");
        estado.put("mensajeriaAsincrona", "RabbitMQ (clúster de 2 nodos)");
        estado.put("streamingEventos", "Apache Kafka (KRaft mode)");
        estado.put("topicosKafka", Map.of(
                "eventos", "orders.events (3 particiones)",
                "auditoria", "audit.timeline (3 particiones)",
                "dlt", "orders.events.DLT (Dead Letter Topic)"
        ));
        estado.put("consumerGroup", "pedidos360-audit-group");
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
