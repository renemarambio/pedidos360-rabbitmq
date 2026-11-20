package cl.duoc.dsy1107.pedidos360.mensajeria.evento;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Envelope común para eventos transmitidos vía Apache Kafka en Pedidos360.
 *
 * Contiene metadatos de mensajería (eventId, timestamp, tipoEvento, traceId)
 * y el cuerpo del evento dentro de 'payloadPedido'.
 */
public record PedidoEventoDTO(
        String eventId,
        String timestamp,
        String tipoEvento,
        Map<String, Object> payloadPedido,
        String traceId
) implements Serializable {

    public PedidoEventoDTO {
        if (eventId == null || eventId.isBlank()) {
            eventId = UUID.randomUUID().toString();
        }
        if (timestamp == null || timestamp.isBlank()) {
            timestamp = Instant.now().toString();
        }
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString();
        }
        payloadPedido = payloadPedido == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(payloadPedido));
    }

    /**
     * Factoría de conveniencia para crear un envelope de evento Kafka.
     *
     * @param tipoEvento Nombre del evento (ej: OrderCreated, OrderAccepted, OrderPreparing, OrderDispatched, OrderDelivered, OrderCancelled)
     * @param pedidoId ID numérico del pedido
     * @param payload Mapa con los detalles de la orden
     * @param traceId ID de correlación y trazabilidad distribuida (opcional)
     */
    public static PedidoEventoDTO crear(String tipoEvento, Long pedidoId, Map<String, Object> payload, String traceId) {
        Map<String, Object> datos = payload != null ? new LinkedHashMap<>(payload) : new LinkedHashMap<>();
        if (pedidoId != null) {
            datos.putIfAbsent("pedidoId", pedidoId);
        }
        return new PedidoEventoDTO(
                UUID.randomUUID().toString(),
                Instant.now().toString(),
                tipoEvento,
                datos,
                (traceId != null && !traceId.isBlank()) ? traceId : UUID.randomUUID().toString()
        );
    }

    /**
     * Extrae el ID del pedido del payload si está presente.
     */
    public Long obtenerPedidoId() {
        if (payloadPedido == null) {
            return null;
        }
        Object id = payloadPedido.get("pedidoId");
        if (id instanceof Number n) {
            return n.longValue();
        }
        if (id instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException ignored) {
            }
        }
        return null;
    }
}
