package cl.duoc.dsy1107.pedidos360.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración de infraestructura para Apache Kafka.
 * Declara programáticamente los tópicos requeridos por la arquitectura de eventos Pedidos360
 * con 3 particiones, factor de replicación 3 (RF=3), políticas de retención explícitas
 * y estrategia de reintentos hacia Dead Letter Topic (DLT).
 */
@EnableKafka
@Configuration
public class KafkaInfraestructuraConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaInfraestructuraConfig.class);

    @Value("${spring.kafka.bootstrap-servers:${pedidos360.kafka.bootstrap-servers:localhost:9092}}")
    private String bootstrapServers;

    @Value("${pedidos360.kafka.topicos.orders-events:orders.events}")
    private String topicoOrdersEvents;

    @Value("${pedidos360.kafka.topicos.audit-timeline:audit.timeline}")
    private String topicoAuditTimeline;

    @Value("${pedidos360.kafka.topicos.orders-events-dlt:orders.events.DLT}")
    private String topicoOrdersEventsDlt;

    @Value("${pedidos360.kafka.particiones:3}")
    private int particiones;

    @Value("${pedidos360.kafka.replicas:3}")
    private short replicas;

    /**
     * Cliente de administración para crear tópicos de forma programática.
     * fatalIfBrokerNotAvailable=false evita que la aplicación falle al iniciar si Kafka aún está arrancando.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setFatalIfBrokerNotAvailable(false);
        return admin;
    }

    /**
     * Cliente de administración para operaciones programáticas sobre el clúster Kafka.
     */
    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    /**
     * Tópico de streaming de eventos de órdenes de negocio: 'orders.events'.
     * Configurado con 3 particiones, 3 réplicas (RF=3) y retención de 7 días.
     */
    @Bean
    public NewTopic ordersEventsTopic() {
        log.info("[KAFKA-CONFIG] Declarando tópico '{}' (particiones={}, réplicas={})",
                topicoOrdersEvents, particiones, replicas);
        return TopicBuilder.name(topicoOrdersEvents)
                .partitions(particiones)
                .replicas(replicas)
                .config("retention.ms", "604800000")
                .config("cleanup.policy", "delete")
                .build();
    }

    /**
     * Tópico de auditoría y trazabilidad histórica: 'audit.timeline'.
     * Configurado con 3 particiones, 3 réplicas (RF=3) y retención de 30 días.
     */
    @Bean
    public NewTopic auditTimelineTopic() {
        log.info("[KAFKA-CONFIG] Declarando tópico '{}' (particiones={}, réplicas={})",
                topicoAuditTimeline, particiones, replicas);
        return TopicBuilder.name(topicoAuditTimeline)
                .partitions(particiones)
                .replicas(replicas)
                .config("retention.ms", "2592000000")
                .config("cleanup.policy", "delete")
                .build();
    }

    /**
     * Tópico Dead Letter Topic (DLT): 'orders.events.DLT'.
     * Recibe los mensajes no procesables tras agotar los reintentos automáticos (3 particiones, RF=3).
     */
    @Bean
    public NewTopic ordersEventsDltTopic() {
        log.info("[KAFKA-CONFIG] Declarando tópico DLT '{}' (particiones={}, réplicas={})",
                topicoOrdersEventsDlt, particiones, replicas);
        return TopicBuilder.name(topicoOrdersEventsDlt)
                .partitions(particiones)
                .replicas(replicas)
                .config("retention.ms", "1209600000")
                .config("cleanup.policy", "delete")
                .build();
    }

    /**
     * Manejador de errores con estrategia de reintentos y redirección hacia DLT.
     * Configuración:
     * - Reintentos: 2 intentos adicionales (3 totales) con pausa de 1 segundo (1000ms).
     * - En caso de agotar reintentos: el DeadLetterPublishingRecoverer redirige al tópico DLT correspondiente.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaOperations,
                (record, exception) -> {
                    String dltTopic = record.topic() + ".DLT";
                    log.warn("[KAFKA-DLT-ROUTER] Agotados los reintentos. Desviando mensaje a DLT '{}' | partición: {} | key: {} | causa: {}",
                            dltTopic, record.partition(), record.key(), exception.getMessage());
                    return new TopicPartition(dltTopic, record.partition());
                }
        );

        // 2 reintentos con intervalo de 1000 ms (FixedBackOff)
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> log.warn(
                "[KAFKA-REINTENTO] Intento #{} fallido en tópico '{}', partición {}, offset {}. Causa: {}",
                deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));

        return errorHandler;
    }

    /**
     * Factoría de contenedores de listeners Kafka con el errorHandler y reintentos DLT inyectados.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }
}