package cl.duoc.dsy1107.pedidos360.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Infraestructura de conexión con RabbitMQ: conversor JSON, plantilla de publicación
 * y fábrica de consumidores con ACK manual.
 */
@Configuration
public class RabbitInfraestructuraConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitInfraestructuraConfig.class);

    @Bean
    public Jackson2JsonMessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * Plantilla de publicación.
     * - mandatory=true: si un mensaje no tiene cola de destino, el broker lo devuelve y queda en el log.
     * - confirm callback: registra si el broker rechazó (NACK) una publicación.
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         Jackson2JsonMessageConverter jsonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);
        template.setMandatory(true);
        template.setReturnsCallback(devuelto -> log.error(
                "[PUBLICACION] Mensaje NO enrutado: exchange='{}' routingKey='{}' motivo='{}' ({})",
                devuelto.getExchange(), devuelto.getRoutingKey(),
                devuelto.getReplyText(), devuelto.getReplyCode()));
        template.setConfirmCallback((correlacion, ack, causa) -> {
            if (!ack) {
                log.error("[PUBLICACION] El broker rechazó la publicación: {}", causa);
            }
        });
        return template;
    }

    /**
     * Fábrica usada por todos los @RabbitListener.
     * ACK manual: cada consumidor decide explícitamente ACK, reintento o NACK hacia la DLQ.
     * prefetch=1: el consumidor recibe un mensaje a la vez.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(1);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}
