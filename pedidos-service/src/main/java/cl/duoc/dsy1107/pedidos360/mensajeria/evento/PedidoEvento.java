package cl.duoc.dsy1107.pedidos360.mensajeria.evento;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Evento "pedido aceptado" que viaja por RabbitMQ en formato JSON.
 * Es inmutable: cada dominio lo recibe y lo procesa sin modificarlo.
 *
 * Los campos falloX permiten simular errores en el laboratorio para
 * demostrar reintentos y envío a DLQ.
 */
public record PedidoEvento(
        String eventoId,
        Long pedidoId,
        String clienteEmail,
        BigDecimal total,
        boolean falloNotificaciones,
        boolean falloCocina,
        boolean falloDocumentos,
        Map<String, Object> metadatos
) {

    public PedidoEvento {
        metadatos = metadatos == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(metadatos));
    }
}
