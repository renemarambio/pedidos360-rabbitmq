package cl.duoc.dsy1107.pedidos360.pedidos.service;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.TipoEventoPedido;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.KafkaPedidoEventPublisher;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.PedidoEventPublisher;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.CrearPedidoRequest;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.PedidoSolicitud;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Lógica de negocio de pedidos.
 * Coordina la mensajería asíncrona (RabbitMQ) y el streaming de eventos (Apache Kafka).
 */
@Service
public class PedidoService {

    private static final Logger log = LoggerFactory.getLogger(PedidoService.class);
    private static final BigDecimal TOTAL_POR_DEFECTO = BigDecimal.valueOf(15990);

    private final PedidoEventPublisher rabbitPublisher;
    private final KafkaPedidoEventPublisher kafkaPublisher;

    public PedidoService(PedidoEventPublisher rabbitPublisher, KafkaPedidoEventPublisher kafkaPublisher) {
        this.rabbitPublisher = rabbitPublisher;
        this.kafkaPublisher = kafkaPublisher;
    }

    /** Pedido creado desde el catálogo web. Emite evento OrderCreated a Kafka y distribuye a RabbitMQ. */
    public PedidoEvento crearPedido(CrearPedidoRequest request) {
        String idPedido = request.idPedido() != null
                ? request.idPedido()
                : "PED-" + ThreadLocalRandom.current().nextInt(1000, 10000);
        long pedidoId = Long.parseLong(idPedido.substring("PED-".length()));
        BigDecimal total = request.precio().multiply(BigDecimal.valueOf(request.cantidad()));

        Map<String, Object> metadatos = new LinkedHashMap<>();
        metadatos.put("origen", "FRONTEND_CATALOGO");
        metadatos.put("timestamp", request.fecha() != null ? request.fecha() : LocalDateTime.now().toString());
        metadatos.put("idPedido", idPedido);
        metadatos.put("productoId", request.productoId());
        metadatos.put("nombreProducto", request.nombreProducto());
        metadatos.put("cantidad", request.cantidad());
        metadatos.put("precioUnitario", request.precio());
        metadatos.put("total", total);

        PedidoEvento evento = new PedidoEvento(
                UUID.randomUUID().toString(),
                pedidoId,
                "cliente-" + pedidoId + "@pedidos360.cl",
                total,
                request.falloNotificaciones(),
                request.falloCocina(),
                request.falloDocumentos(),
                metadatos);

        // 1. Streaming de evento OrderCreated a Apache Kafka con espera síncrona de ACK
        PedidoEventoDTO kafkaEvento = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_CREATED.getCodigo(),
                pedidoId,
                metadatos,
                evento.eventoId()
        );
        try {
            kafkaPublisher.publicarEventoPedido(kafkaEvento).get(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            log.error("[PEDIDO-SERVICE] Error esperando ack de Kafka para OrderCreated de pedidoId {}: {}", pedidoId, ex.getMessage(), ex);
            throw new RuntimeException("Error al publicar evento OrderCreated en Kafka", ex);
        }

        // 2. Publicación en RabbitMQ SOLO después de confirmar persistencia en Kafka
        rabbitPublisher.publicarPedidoAceptado(evento);

        return evento;
    }

    /** Pedido de laboratorio para probar el flujo y los fallos controlados. Emite OrderAccepted. */
    public PedidoEvento aceptarPedido(PedidoSolicitud solicitud) {
        long pedidoId = solicitud.pedidoId() != null
                ? solicitud.pedidoId()
                : ThreadLocalRandom.current().nextLong(10000, 100000);
        String email = solicitud.clienteEmail() != null && !solicitud.clienteEmail().isBlank()
                ? solicitud.clienteEmail()
                : "cliente" + pedidoId + "@pedidos360.cl";
        BigDecimal total = solicitud.total() != null ? solicitud.total() : TOTAL_POR_DEFECTO;

        Map<String, Object> metadatos = new LinkedHashMap<>();
        metadatos.put("origen", "API_LAB");
        metadatos.put("timestamp", LocalDateTime.now().toString());
        metadatos.put("total", total);

        PedidoEvento evento = new PedidoEvento(
                UUID.randomUUID().toString(),
                pedidoId,
                email,
                total,
                solicitud.falloNotificaciones(),
                solicitud.falloCocina(),
                solicitud.falloDocumentos(),
                metadatos);

        // 1. Streaming de evento OrderAccepted a Apache Kafka con espera síncrona de ACK
        PedidoEventoDTO kafkaEvento = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_ACCEPTED.getCodigo(),
                pedidoId,
                metadatos,
                evento.eventoId()
        );
        try {
            kafkaPublisher.publicarEventoPedido(kafkaEvento).get(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            log.error("[PEDIDO-SERVICE] Error esperando ack de Kafka para OrderAccepted de pedidoId {}: {}", pedidoId, ex.getMessage(), ex);
            throw new RuntimeException("Error al publicar evento OrderAccepted en Kafka", ex);
        }

        // 2. Publicación en RabbitMQ SOLO después de confirmar persistencia en Kafka
        rabbitPublisher.publicarPedidoAceptado(evento);

        return evento;
    }

    /**
     * Actualiza el estado de un pedido y emite el evento correspondiente a Kafka.
     * La clave (Key) es pedidoId, garantizando el orden cronológico en la misma partición.
     */
    public PedidoEventoDTO actualizarEstadoPedido(Long pedidoId, String tipoEvento, Map<String, Object> detalles) {
        Map<String, Object> payload = detalles != null ? new LinkedHashMap<>(detalles) : new LinkedHashMap<>();
        payload.putIfAbsent("pedidoId", pedidoId);
        payload.putIfAbsent("actualizadoEn", LocalDateTime.now().toString());

        PedidoEventoDTO evento = PedidoEventoDTO.crear(
                tipoEvento,
                pedidoId,
                payload,
                UUID.randomUUID().toString()
        );

        try {
            kafkaPublisher.publicarEventoPedido(evento).get(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            log.error("[PEDIDO-SERVICE] Error esperando ack de Kafka para {} de pedidoId {}: {}", tipoEvento, pedidoId, ex.getMessage(), ex);
            throw new RuntimeException("Error al publicar evento " + tipoEvento + " en Kafka", ex);
        }
        return evento;
    }

    /** Emite evento OrderDelivered a Kafka. */
    public PedidoEventoDTO entregarPedido(Long pedidoId, Map<String, Object> detalles) {
        return actualizarEstadoPedido(pedidoId, TipoEventoPedido.ORDER_DELIVERED.getCodigo(), detalles);
    }

    /** Emite evento OrderCancelled a Kafka. */
    public PedidoEventoDTO cancelarPedido(Long pedidoId, String motivo) {
        Map<String, Object> detalles = new LinkedHashMap<>();
        detalles.put("motivoCancelacion", motivo != null ? motivo : "Cancelado por el usuario");
        return actualizarEstadoPedido(pedidoId, TipoEventoPedido.ORDER_CANCELLED.getCodigo(), detalles);
    }

    /** Emite evento con flag de error forzado para probar reintentos y DLT en auditoría Kafka. */
    public PedidoEventoDTO simularFalloDlt(Long pedidoId) {
        Map<String, Object> detalles = new LinkedHashMap<>();
        detalles.put("simularErrorAuditoria", true);
        detalles.put("motivo", "Demostración de reintentos y desvío a DLT");
        return actualizarEstadoPedido(pedidoId, "OrderAuditErrorSimulation", detalles);
    }

    /** Publica un evento de monitoreo en RabbitMQ topic exchange. */
    public String publicarEventoMonitoreo(String routingKey, Map<String, Object> payload) {
        Map<String, Object> datos = payload != null ? new LinkedHashMap<>(payload) : new LinkedHashMap<>();
        datos.putIfAbsent("timestamp", LocalDateTime.now().toString());
        return rabbitPublisher.publicarEventoMonitoreo(routingKey, datos);
    }
}
