package cl.duoc.dsy1107.pedidos360.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología de RabbitMQ: declara todos los exchanges, colas y bindings como beans.
 * Spring (RabbitAdmin) los crea en el broker al conectarse.
 *
 * Rutas de mensajería por caso de uso:
 *
 * 1) Pedido aceptado -> dominio (notificaciones / cocina / documentos)
 *    pedidos360.direct.exchange --[routing-key del dominio]--> cola del dominio
 *
 * 2) Reintento de un error recuperable
 *    consumidor --> direct.exchange --[*.retry]--> cola retry (TTL)
 *    al vencer el TTL --> direct.exchange --[routing-key]--> cola del dominio
 *
 * 3) Mensaje rechazado (sin reintentos o error no recuperable)
 *    cola del dominio --(NACK, dead-letter)--> pedidos360.dlx --[*.failed]--> DLQ del dominio
 *
 * 4) Monitoreo
 *    pedidos360.topic.exchange --[pedidos.#]--> cola de monitoreo
 *
 * Los nombres salen de {@link MensajeriaProperties} (application.yml).
 */
@Configuration
public class RabbitTopologiaConfig {

    private final MensajeriaProperties props;

    public RabbitTopologiaConfig(MensajeriaProperties props) {
        this.props = props;
    }

    // =====================================================================
    // EXCHANGES
    // =====================================================================

    /** Direct exchange: enruta cada pedido a la cola exacta de cada dominio. */
    @Bean
    public DirectExchange pedidosDirectExchange() {
        return new DirectExchange(props.exchanges().direct(), true, false);
    }

    /** Topic exchange: eventos de monitoreo enrutados por patrón (pedidos.#). */
    @Bean
    public TopicExchange pedidosTopicExchange() {
        return new TopicExchange(props.exchanges().topic(), true, false);
    }

    /** Dead letter exchange: recibe los mensajes rechazados y los envía a la DLQ de su dominio. */
    @Bean
    public DirectExchange pedidosDeadLetterExchange() {
        return new DirectExchange(props.exchanges().deadLetter(), true, false);
    }

    // =====================================================================
    // DOMINIO NOTIFICACIONES (correo de confirmación al cliente)
    // =====================================================================

    @Bean
    public Queue notificacionesQueue() {
        return colaPrincipal(props.notificaciones());
    }

    @Bean
    public Queue notificacionesRetryQueue() {
        return colaReintento(props.notificaciones());
    }

    @Bean
    public Queue notificacionesDlq() {
        return colaDlq(props.notificaciones());
    }

    @Bean
    public Binding notificacionesBinding() {
        return BindingBuilder.bind(notificacionesQueue()).to(pedidosDirectExchange())
                .with(props.notificaciones().routingKey());
    }

    @Bean
    public Binding notificacionesRetryBinding() {
        return BindingBuilder.bind(notificacionesRetryQueue()).to(pedidosDirectExchange())
                .with(props.notificaciones().retryRoutingKey());
    }

    @Bean
    public Binding notificacionesDlqBinding() {
        return BindingBuilder.bind(notificacionesDlq()).to(pedidosDeadLetterExchange())
                .with(props.notificaciones().deadLetterRoutingKey());
    }

    // =====================================================================
    // DOMINIO COCINA (ticket de preparación)
    // =====================================================================

    @Bean
    public Queue cocinaQueue() {
        return colaPrincipal(props.cocina());
    }

    @Bean
    public Queue cocinaRetryQueue() {
        return colaReintento(props.cocina());
    }

    @Bean
    public Queue cocinaDlq() {
        return colaDlq(props.cocina());
    }

    @Bean
    public Binding cocinaBinding() {
        return BindingBuilder.bind(cocinaQueue()).to(pedidosDirectExchange())
                .with(props.cocina().routingKey());
    }

    @Bean
    public Binding cocinaRetryBinding() {
        return BindingBuilder.bind(cocinaRetryQueue()).to(pedidosDirectExchange())
                .with(props.cocina().retryRoutingKey());
    }

    @Bean
    public Binding cocinaDlqBinding() {
        return BindingBuilder.bind(cocinaDlq()).to(pedidosDeadLetterExchange())
                .with(props.cocina().deadLetterRoutingKey());
    }

    // =====================================================================
    // DOMINIO DOCUMENTOS (boleta del pedido)
    // =====================================================================

    @Bean
    public Queue documentosQueue() {
        return colaPrincipal(props.documentos());
    }

    @Bean
    public Queue documentosRetryQueue() {
        return colaReintento(props.documentos());
    }

    @Bean
    public Queue documentosDlq() {
        return colaDlq(props.documentos());
    }

    @Bean
    public Binding documentosBinding() {
        return BindingBuilder.bind(documentosQueue()).to(pedidosDirectExchange())
                .with(props.documentos().routingKey());
    }

    @Bean
    public Binding documentosRetryBinding() {
        return BindingBuilder.bind(documentosRetryQueue()).to(pedidosDirectExchange())
                .with(props.documentos().retryRoutingKey());
    }

    @Bean
    public Binding documentosDlqBinding() {
        return BindingBuilder.bind(documentosDlq()).to(pedidosDeadLetterExchange())
                .with(props.documentos().deadLetterRoutingKey());
    }

    // =====================================================================
    // MONITOREO (topic exchange)
    // =====================================================================

    @Bean
    public Queue monitoreoQueue() {
        return QueueBuilder.durable(props.monitoreo().queue()).build();
    }

    @Bean
    public Binding monitoreoBinding() {
        return BindingBuilder.bind(monitoreoQueue()).to(pedidosTopicExchange())
                .with(props.monitoreo().bindingPattern());
    }

    // =====================================================================
    // Constructores comunes
    // =====================================================================

    /** Cola principal: si un mensaje se rechaza sin requeue, RabbitMQ lo envía a la DLX. */
    private Queue colaPrincipal(MensajeriaProperties.Dominio dominio) {
        return QueueBuilder.durable(dominio.queue())
                .deadLetterExchange(props.exchanges().deadLetter())
                .deadLetterRoutingKey(dominio.deadLetterRoutingKey())
                .build();
    }

    /**
     * DLQ con retención: los mensajes muertos duran como máximo el TTL configurado y,
     * si se supera el largo máximo, se descartan los más antiguos (drop-head).
     */
    private Queue colaDlq(MensajeriaProperties.Dominio dominio) {
        return QueueBuilder.durable(dominio.dlq())
                .ttl((int) props.retencionDlq().ttlMs())
                .maxLength(props.retencionDlq().maxLength())
                .overflow(QueueBuilder.Overflow.dropHead)
                .build();
    }

    /** Cola de reintento: retiene el mensaje durante el TTL y luego lo devuelve a la cola principal. */
    private Queue colaReintento(MensajeriaProperties.Dominio dominio) {
        return QueueBuilder.durable(dominio.retryQueue())
                .ttl((int) props.reintentos().ttlMs())
                .deadLetterExchange(props.exchanges().direct())
                .deadLetterRoutingKey(dominio.routingKey())
                .build();
    }
}
