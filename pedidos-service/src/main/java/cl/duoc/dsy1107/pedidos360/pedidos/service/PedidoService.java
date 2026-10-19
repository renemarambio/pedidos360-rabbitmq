package cl.duoc.dsy1107.pedidos360.pedidos.service;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.PedidoEventPublisher;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.CrearPedidoRequest;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.PedidoSolicitud;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Lógica de negocio de pedidos: arma el evento "pedido aceptado" y lo entrega al publicador.
 * Depende solo de la interfaz {@link PedidoEventPublisher}; no conoce RabbitMQ.
 */
@Service
public class PedidoService {

    private static final BigDecimal TOTAL_POR_DEFECTO = BigDecimal.valueOf(15990);

    private final PedidoEventPublisher publisher;

    public PedidoService(PedidoEventPublisher publisher) {
        this.publisher = publisher;
    }

    /** Pedido creado desde el catálogo web. */
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

        PedidoEvento evento = new PedidoEvento(
                UUID.randomUUID().toString(),
                pedidoId,
                "cliente-" + pedidoId + "@pedidos360.cl",
                total,
                request.falloNotificaciones(),
                request.falloCocina(),
                request.falloDocumentos(),
                metadatos);

        publisher.publicarPedidoAceptado(evento);
        return evento;
    }

    /** Pedido de laboratorio para probar el flujo y los fallos controlados. */
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

        PedidoEvento evento = new PedidoEvento(
                UUID.randomUUID().toString(),
                pedidoId,
                email,
                total,
                solicitud.falloNotificaciones(),
                solicitud.falloCocina(),
                solicitud.falloDocumentos(),
                metadatos);

        publisher.publicarPedidoAceptado(evento);
        return evento;
    }

    /** Publica un evento de monitoreo. Devuelve la routing key usada. */
    public String publicarEventoMonitoreo(String routingKey, Map<String, Object> payload) {
        Map<String, Object> datos = payload != null ? new LinkedHashMap<>(payload) : new LinkedHashMap<>();
        datos.putIfAbsent("timestamp", LocalDateTime.now().toString());
        return publisher.publicarEventoMonitoreo(routingKey, datos);
    }
}
