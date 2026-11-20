package cl.duoc.dsy1107.pedidos360.mensajeria.publicacion;

import cl.duoc.dsy1107.pedidos360.config.MensajeriaProperties;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Productor RabbitMQ: implementa {@link PedidoEventPublisher}.
 * Publica mensajes JSON persistentes usando los nombres de {@link MensajeriaProperties}.
 */
@Component
public class RabbitPedidoEventPublisher implements PedidoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RabbitPedidoEventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final MensajeriaProperties props;

    public RabbitPedidoEventPublisher(RabbitTemplate rabbitTemplate, MensajeriaProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    @Override
    public void publicarPedidoAceptado(PedidoEvento evento) {
        List<MensajeriaProperties.Dominio> dominios =
                List.of(props.notificaciones(), props.cocina(), props.documentos());

        for (MensajeriaProperties.Dominio dominio : dominios) {
            rabbitTemplate.convertAndSend(props.exchanges().direct(), dominio.routingKey(), evento, mensaje -> {
                MessageProperties p = mensaje.getMessageProperties();
                p.setContentType(MessageProperties.CONTENT_TYPE_JSON);
                p.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                p.setMessageId(evento.eventoId());
                p.setHeader(props.reintentos().headerIntentos(), 0);
                return mensaje;
            });
            log.info("[PRODUCTOR] pedidoId={} publicado en '{}' con routingKey '{}'",
                    evento.pedidoId(), props.exchanges().direct(), dominio.routingKey());
        }
    }

    @Override
    public String publicarEventoMonitoreo(String routingKey, Object payload) {
        String clave = (routingKey == null || routingKey.isBlank())
                ? props.monitoreo().routingKeyPorDefecto()
                : routingKey;

        rabbitTemplate.convertAndSend(props.exchanges().topic(), clave, payload, mensaje -> {
            mensaje.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_JSON);
            mensaje.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            return mensaje;
        });
        log.info("[PRODUCTOR-TOPIC] Evento publicado en '{}' con routingKey '{}'", props.exchanges().topic(), clave);
        return clave;
    }
}
