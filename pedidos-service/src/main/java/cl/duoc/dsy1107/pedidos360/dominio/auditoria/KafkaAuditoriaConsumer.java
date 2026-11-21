package cl.duoc.dsy1107.pedidos360.dominio.auditoria;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEventoDTO;
import cl.duoc.dsy1107.pedidos360.mensajeria.publicacion.KafkaPedidoEventPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Consumidor de auditoría de eventos de órdenes para Apache Kafka.
 *
 * Escucha el tópico 'orders.events' con el consumer group 'pedidos360-audit-group',
 * extrayendo metadatos esenciales (quién, qué, cuándo, partición, offset)
 * y emitiendo un registro estructurado a la línea de tiempo de auditoría.
 */
@Component
public class KafkaAuditoriaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaAuditoriaConsumer.class);

    private final KafkaPedidoEventPublisher kafkaPublisher;

    public KafkaAuditoriaConsumer(KafkaPedidoEventPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    /**
     * Consumo principal de eventos de órdenes.
     * Registra auditoría completa: quién/qué/cuándo/partición/offset.
     */
    @KafkaListener(
            topics = "${pedidos360.kafka.topicos.orders-events:orders.events}",
            groupId = "${pedidos360.kafka.group-id:pedidos360-audit-group}",
            concurrency = "3",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void procesarEventoPedido(
            @Payload PedidoEventoDTO evento,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer particion,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            @Header(value = KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long timestampRecord
    ) {
        // Soporte de laboratorio para probar el mecanismo de reintentos y desvío a DLT
        if (evento.payloadPedido() != null && Boolean.TRUE.equals(evento.payloadPedido().get("simularErrorAuditoria"))) {
            log.warn("[KAFKA-AUDITORIA] SIMULACIÓN ERROR: Fallo provocado intencionalmente para pedidoId={} (disparando reintentos -> DLT)", key);
            throw new RuntimeException("Error forzado de auditoría para demostración de DLT (pedidoId=" + key + ")");
        }

        String fechaKafka = timestampRecord != null ? Instant.ofEpochMilli(timestampRecord).toString() : "N/A";
        String particionStr = particion != null ? String.valueOf(particion) : "N/A";
        String offsetStr = offset != null ? String.valueOf(offset) : "N/A";

        log.info("""
                ====================== AUDITORÍA KAFKA ======================
                Tópico:            orders.events
                Partición:         {}
                Offset:            {}
                Clave (Key):       {}
                Tipo de Evento:    {}
                ID Pedido:         {}
                Event ID:          {}
                Trace ID:          {}
                Timestamp Evento:  {}
                Timestamp Broker:  {}
                Payload Resumen:   {}
                =============================================================""",
                particionStr, offsetStr, key,
                evento.tipoEvento(), evento.obtenerPedidoId(),
                evento.eventId(), evento.traceId(),
                evento.timestamp(), fechaKafka,
                evento.payloadPedido()
        );

        // Envío complementario al tópico audit.timeline para trazabilidad centralizada
        Map<String, Object> timelineData = new LinkedHashMap<>();
        timelineData.put("timelineId", evento.eventId());
        timelineData.put("tipoEvento", evento.tipoEvento());
        timelineData.put("pedidoId", evento.obtenerPedidoId());
        timelineData.put("particionOrigen", particion);
        timelineData.put("offsetOrigen", offset);
        timelineData.put("traceId", evento.traceId());
        timelineData.put("registradoEn", Instant.now().toString());

        kafkaPublisher.publicarAuditTimeline(key != null ? key : evento.eventId(), timelineData);
    }

    /**
     * Consumidor para el Dead Letter Topic (DLT): registra mensajes no recuperables tras reintentos.
     */
    @KafkaListener(
            topics = "${pedidos360.kafka.topicos.orders-events-dlt:orders.events.DLT}",
            groupId = "${pedidos360.kafka.group-id:pedidos360-audit-group}-dlt",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void procesarDlt(
            ConsumerRecord<String, Object> record,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer particion,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset
    ) {
        log.error("""
                !!!!!!!!!!!!!!!!!! KAFKA DEAD LETTER TOPIC (DLT) !!!!!!!!!!!!!!!!!!
                Tópico DLT:        {}
                Partición:         {}
                Offset:            {}
                Clave (Key):       {}
                Valor/Payload:     {}
                Headers:           {}
                !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!""",
                record.topic(), particion, offset, record.key(), record.value(), record.headers()
        );
    }
}
