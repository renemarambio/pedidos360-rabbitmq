package cl.duoc.dsy1107.pedidos360.dominio.documentos;

import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ErrorProcesamientoException;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * Lógica de negocio del dominio Documentos: emite la boleta del pedido.
 */
@Service
public class DocumentoService {

    private static final Logger log = LoggerFactory.getLogger(DocumentoService.class);

    public void generarBoleta(PedidoEvento evento) {
        if (evento.total() == null || evento.total().compareTo(BigDecimal.ZERO) <= 0) {
            throw ErrorProcesamientoException.noRecuperable("no se puede emitir boleta con total " + evento.total());
        }
        if (evento.falloDocumentos()) {
            throw ErrorProcesamientoException.recuperable("servicio de boletas no responde (fallo simulado)");
        }
        log.info("[DOCUMENTOS] Boleta BOL-{} emitida por ${}", evento.pedidoId(), evento.total());
    }
}
