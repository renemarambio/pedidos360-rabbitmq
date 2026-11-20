package cl.duoc.dsy1107.pedidos360.dominio.auditoria;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Auditoría de mensajes muertos: lee las DLQ de los tres dominios y registra en el log
 * por qué falló cada mensaje (cola de origen, motivo y cantidad de rechazos).
 *
 * Se activa con pedidos360.rabbit.auditoria-dlq.habilitada. Si está desactivado,
 * los mensajes quedan acumulados en las DLQ para revisarlos en la consola de RabbitMQ.
 */
@Component
public class DeadLetterAuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterAuditConsumer.class);

    @RabbitListener(
            id = "auditor-dlq",
            queues = {
                    "${pedidos360.rabbit.notificaciones.dlq}",
                    "${pedidos360.rabbit.cocina.dlq}",
                    "${pedidos360.rabbit.documentos.dlq}"
            },
            autoStartup = "${pedidos360.rabbit.auditoria-dlq.habilitada}")
    public void registrar(Message mensaje, Channel canal) throws IOException {
        long deliveryTag = mensaje.getMessageProperties().getDeliveryTag();
        String dlq = mensaje.getMessageProperties().getConsumerQueue();
        String colaOrigen = "desconocida";
        Object motivo = "desconocido";
        Object rechazos = 0;

        List<Map<String, ?>> xDeath = mensaje.getMessageProperties().getXDeathHeader();
        if (xDeath != null && !xDeath.isEmpty()) {
            Map<String, ?> ultimo = xDeath.get(0);
            colaOrigen = String.valueOf(ultimo.get("queue"));
            motivo = ultimo.get("reason");
            rechazos = ultimo.get("count");
        }

        log.error("[DLQ] Mensaje muerto en '{}' | origen='{}' | motivo={} | rechazos={} | messageId={} | cuerpo={}",
                dlq, colaOrigen, motivo, rechazos,
                mensaje.getMessageProperties().getMessageId(),
                new String(mensaje.getBody(), StandardCharsets.UTF_8));

        // Queda registrado en el log; se confirma para no volver a leerlo.
        canal.basicAck(deliveryTag, false);
    }
}
