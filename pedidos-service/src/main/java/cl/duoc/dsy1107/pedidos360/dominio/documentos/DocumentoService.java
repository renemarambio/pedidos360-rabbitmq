package cl.duoc.dsy1107.pedidos360.dominio.documentos;

import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ErrorProcesamientoException;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.TipoEventoPedido;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.KafkaPedidoEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lógica de negocio del dominio Documentos: emite la boleta del pedido
 * y emite el evento de streaming OrderDispatched a Apache Kafka (evento emitido por dominio).
 */
@Service
public class DocumentoService {

    private static final Logger log = LoggerFactory.getLogger(DocumentoService.class);

    private final KafkaPedidoEventPublisher kafkaPublisher;
    private final Set<Long> boletasGeneradas = ConcurrentHashMap.newKeySet();

    public DocumentoService(KafkaPedidoEventPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    public void generarBoleta(PedidoEvento evento) {
        if (evento.total() == null || evento.total().compareTo(BigDecimal.ZERO) <= 0) {
            throw ErrorProcesamientoException.noRecuperable("no se puede emitir boleta con total " + evento.total());
        }
        if (evento.falloDocumentos()) {
            throw ErrorProcesamientoException.recuperable("servicio de boletas no responde (fallo simulado)");
        }
        log.info("[DOCUMENTOS] Boleta BOL-{} emitida por ${}", evento.pedidoId(), evento.total());

        // Si el evento en RabbitMQ indica estado de despacho, se emite OrderDispatched a Kafka
        if ("OrderDispatched".equals(evento.metadatos().get("estado"))) {
            if (!boletasGeneradas.add(evento.pedidoId())) {
                log.warn("[DOCUMENTOS] Boleta para pedidoId={} ya emitida previamente. Omitiendo duplicado de OrderDispatched en Kafka.", evento.pedidoId());
                return;
            }

            Map<String, Object> payload = new LinkedHashMap<>(evento.metadatos());
            payload.put("boletaId", "BOL-" + evento.pedidoId());
            payload.put("total", evento.total());
            payload.put("estado", TipoEventoPedido.ORDER_DISPATCHED.getCodigo());

            PedidoEventoDTO eventoKafka = PedidoEventoDTO.crear(
                    TipoEventoPedido.ORDER_DISPATCHED.getCodigo(),
                    evento.pedidoId(),
                    payload,
                    evento.eventoId()
            );
            kafkaPublisher.publicarEventoPedido(eventoKafka);
        }
    }
}
