package cl.duoc.dsy1107.pedidos360.dominio.monitoreo;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Consumidor de observabilidad: recibe todo lo publicado en el topic exchange con "pedidos.#".
 *
 * Decisión de ACK: siempre confirma. Es un registro informativo; reprocesarlo o
 * mandarlo a una DLQ no aporta al negocio y solo generaría ruido.
 */
@Component
public class MonitoreoConsumer {

    private static final Logger log = LoggerFactory.getLogger(MonitoreoConsumer.class);

    @RabbitListener(id = "consumidor-monitoreo", queues = "${pedidos360.rabbit.monitoreo.queue}")
    public void recibir(Message mensaje, Channel canal) throws IOException {
        long deliveryTag = mensaje.getMessageProperties().getDeliveryTag();
        try {
            log.info("[MONITOREO] exchange='{}' routingKey='{}' payload={}",
                    mensaje.getMessageProperties().getReceivedExchange(),
                    mensaje.getMessageProperties().getReceivedRoutingKey(),
                    new String(mensaje.getBody(), StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            log.warn("[MONITOREO] No se pudo registrar el evento: {}", e.getMessage());
        } finally {
            canal.basicAck(deliveryTag, false);
        }
    }
}
