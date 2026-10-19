package cl.duoc.dsy1107.pedidos360.dominio.cocina;

import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ErrorProcesamientoException;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Lógica de negocio del dominio Cocina: genera el ticket de preparación del pedido.
 */
@Service
public class CocinaService {

    private static final Logger log = LoggerFactory.getLogger(CocinaService.class);

    public void generarTicket(PedidoEvento evento) {
        if (evento.falloCocina()) {
            throw ErrorProcesamientoException.recuperable("impresora de cocina sin conexión (fallo simulado)");
        }
        Object producto = evento.metadatos().getOrDefault("nombreProducto", "pedido");
        Object cantidad = evento.metadatos().getOrDefault("cantidad", 1);
        log.info("[COCINA] Ticket COC-{} generado: {} x{}", evento.pedidoId(), producto, cantidad);
    }
}
