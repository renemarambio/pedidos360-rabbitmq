package cl.duoc.dsy1107.pedidos360.mensajeria.consumo;

import cl.duoc.dsy1107.pedidos360.config.MensajeriaProperties;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Lógica común de confirmación de mensajes para todos los consumidores de dominio.
 *
 * Decisiones (ACK manual):
 * <pre>
 * | Situación                                   | Acción                                              |
 * |---------------------------------------------|-----------------------------------------------------|
 * | Procesado sin errores                       | basicAck                                            |
 * | Mensaje ilegible o incompleto               | basicNack(requeue=false) -> DLQ (no recuperable)    |
 * | Error no recuperable del dominio            | basicNack(requeue=false) -> DLQ                     |
 * | Error recuperable, quedan intentos          | publica en cola retry (TTL) + basicAck del original |
 * | Error recuperable, intentos agotados        | basicNack(requeue=false) -> DLQ                     |
 * | No se pudo publicar el reintento            | basicNack(requeue=false) -> DLQ (no se pierde)      |
 * </pre>
 * Nunca se usa requeue=true: evita ciclos infinitos con mensajes que siempre fallan.
 */
@Component
public class ProcesadorMensajes {

    private static final Logger log = LoggerFactory.getLogger(ProcesadorMensajes.class);

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final MensajeriaProperties props;

    public ProcesadorMensajes(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper, MensajeriaProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.props = props;
    }

    /**
     * Procesa un mensaje de un dominio.
     *
     * @param dominio       ruta de mensajería del dominio (colas y routing keys)
     * @param mensaje       mensaje AMQP recibido
     * @param canal         canal para confirmar (ACK/NACK)
     * @param logicaNegocio acción del servicio de dominio; lanza {@link ErrorProcesamientoException} si falla
     */
    public void procesar(MensajeriaProperties.Dominio dominio, Message mensaje, Channel canal,
                         Consumer<PedidoEvento> logicaNegocio) throws IOException {
        String etiqueta = dominio.nombre().toUpperCase();
        long deliveryTag = mensaje.getMessageProperties().getDeliveryTag();

        // 1. Leer el evento. Si no se puede, nunca se podrá procesar: directo a la DLQ.
        PedidoEvento evento;
        try {
            evento = leerEvento(mensaje);
        } catch (IOException | IllegalArgumentException e) {
            log.error("[{}] Mensaje inválido, no recuperable: {}. NACK -> DLQ '{}'",
                    etiqueta, e.getMessage(), dominio.dlq());
            canal.basicNack(deliveryTag, false, false);
            return;
        }

        int intentos = obtenerIntentos(mensaje);

        // 2. Ejecutar la lógica del dominio y confirmar según el resultado.
        try {
            logicaNegocio.accept(evento);
            canal.basicAck(deliveryTag, false);
            log.info("[{}] pedidoId={} procesado. ACK (intentos previos={})", etiqueta, evento.pedidoId(), intentos);

        } catch (ErrorProcesamientoException e) {
            if (!e.isRecuperable()) {
                log.error("[{}] pedidoId={} error NO recuperable: {}. NACK -> DLQ '{}'",
                        etiqueta, evento.pedidoId(), e.getMessage(), dominio.dlq());
                canal.basicNack(deliveryTag, false, false);
            } else {
                manejarErrorRecuperable(dominio, mensaje, canal, evento, intentos, e.getMessage());
            }

        } catch (RuntimeException e) {
            // Error inesperado: se trata como temporal y se reintenta.
            log.warn("[{}] pedidoId={} error inesperado: {}", etiqueta, evento.pedidoId(), e.toString());
            manejarErrorRecuperable(dominio, mensaje, canal, evento, intentos, e.getMessage());
        }
    }

    private void manejarErrorRecuperable(MensajeriaProperties.Dominio dominio, Message mensaje, Channel canal,
                                         PedidoEvento evento, int intentos, String causa) throws IOException {
        String etiqueta = dominio.nombre().toUpperCase();
        long deliveryTag = mensaje.getMessageProperties().getDeliveryTag();
        int maxIntentos = props.reintentos().maxIntentos();

        if (intentos >= maxIntentos) {
            log.error("[{}] pedidoId={} reintentos agotados ({}/{}). Causa: {}. NACK -> DLQ '{}'",
                    etiqueta, evento.pedidoId(), intentos, maxIntentos, causa, dominio.dlq());
            canal.basicNack(deliveryTag, false, false);
            return;
        }

        int siguienteIntento = intentos + 1;
        try {
            publicarReintento(dominio, mensaje, siguienteIntento);
        } catch (AmqpException e) {
            log.error("[{}] pedidoId={} no se pudo publicar el reintento: {}. NACK -> DLQ '{}'",
                    etiqueta, evento.pedidoId(), e.getMessage(), dominio.dlq());
            canal.basicNack(deliveryTag, false, false);
            return;
        }

        // El original se confirma solo después de dejar la copia en la cola de reintento.
        canal.basicAck(deliveryTag, false);
        log.warn("[{}] pedidoId={} error recuperable: {}. Reintento {}/{} en '{}' (espera {} ms)",
                etiqueta, evento.pedidoId(), causa, siguienteIntento, maxIntentos,
                dominio.retryQueue(), props.reintentos().ttlMs());
    }

    /** Reenvía el mismo contenido a la cola de reintento del dominio con el contador actualizado. */
    private void publicarReintento(MensajeriaProperties.Dominio dominio, Message original, int intento) {
        Message copia = MessageBuilder.withBody(original.getBody())
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .setMessageId(original.getMessageProperties().getMessageId())
                .setHeader(props.reintentos().headerIntentos(), intento)
                .build();
        rabbitTemplate.send(props.exchanges().direct(), dominio.retryRoutingKey(), copia);
    }

    private PedidoEvento leerEvento(Message mensaje) throws IOException {
        byte[] cuerpo = mensaje.getBody();
        if (cuerpo == null || cuerpo.length == 0) {
            throw new IllegalArgumentException("mensaje sin contenido");
        }
        PedidoEvento evento = objectMapper.readValue(cuerpo, PedidoEvento.class);
        if (evento == null || evento.pedidoId() == null) {
            throw new IllegalArgumentException("el evento no trae pedidoId");
        }
        return evento;
    }

    private int obtenerIntentos(Message mensaje) {
        Object valor = mensaje.getMessageProperties().getHeader(props.reintentos().headerIntentos());
        if (valor instanceof Number numero) {
            return numero.intValue();
        }
        if (valor != null) {
            try {
                return Integer.parseInt(valor.toString());
            } catch (NumberFormatException ignorado) {
                return 0;
            }
        }
        return 0;
    }
}
