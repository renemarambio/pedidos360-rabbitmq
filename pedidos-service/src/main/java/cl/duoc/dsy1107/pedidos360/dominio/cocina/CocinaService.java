package cl.duoc.dsy1107.pedidos360.dominio.cocina;

import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ErrorProcesamientoException;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.TipoEventoPedido;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.KafkaPedidoEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Lógica de negocio del dominio Cocina: genera el ticket de preparación del pedido
 * y emite el evento de streaming OrderPreparing a Apache Kafka.
 */
@Service
public class CocinaService {

    private static final Logger log = LoggerFactory.getLogger(CocinaService.class);

    private final KafkaPedidoEventPublisher kafkaPublisher;

    public CocinaService(KafkaPedidoEventPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    public void generarTicket(PedidoEvento evento) {
        if (evento.falloCocina()) {
            throw ErrorProcesamientoException.recuperable("impresora de cocina sin conexión (fallo simulado)");
        }
        Object producto = evento.metadatos().getOrDefault("nombreProducto", "pedido");
        Object cantidad = evento.metadatos().getOrDefault("cantidad", 1);
        log.info("[COCINA] Ticket COC-{} generado: {} x{}", evento.pedidoId(), producto, cantidad);

        // Streaming de evento OrderPreparing hacia Apache Kafka con key = pedidoId
        Map<String, Object> payload = new LinkedHashMap<>(evento.metadatos());
        payload.put("ticketId", "COC-" + evento.pedidoId());
        payload.put("estado", TipoEventoPedido.ORDER_PREPARING.getCodigo());

        PedidoEventoDTO eventoKafka = PedidoEventoDTO.crear(
                TipoEventoPedido.ORDER_PREPARING.getCodigo(),
                evento.pedidoId(),
                payload,
                evento.eventoId()
        );
        kafkaPublisher.publicarEventoPedido(eventoKafka);
    }
}
