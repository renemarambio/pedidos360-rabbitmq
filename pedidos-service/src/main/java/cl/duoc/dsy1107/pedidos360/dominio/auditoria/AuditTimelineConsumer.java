package cl.duoc.dsy1107.pedidos360.dominio.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Consumidor de eventos de la línea de tiempo de auditoría (audit.timeline).
 *
 * Mantiene un registro en memoria concurrente del historial y último estado
 * de cada pedido para consulta y trazabilidad operacional en tiempo real.
 */
@Component
public class AuditTimelineConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditTimelineConsumer.class);

    // Registro concurrente en memoria: último estado por pedidoId
    private final Map<Long, Map<String, Object>> ultimoEstadoPorPedido = new ConcurrentHashMap<>();

    // Registro concurrente en memoria: lista de todos los eventos registrados por pedidoId
    private final Map<Long, List<Map<String, Object>>> historialEventosPorPedido = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "${pedidos360.kafka.topicos.audit-timeline:audit.timeline}",
            groupId = "pedidos360-timeline-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumirEventoTimeline(
            @Payload Object payload,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topico,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer particion,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            @Header(value = KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long timestampRecord
    ) {
        Object datos = payload;
        if (payload instanceof org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record) {
            datos = record.value();
            if (key == null && record.key() != null) {
                key = String.valueOf(record.key());
            }
            if (topico == null && record.topic() != null) {
                topico = record.topic();
            }
            if (particion == null) {
                particion = record.partition();
            }
            if (offset == null) {
                offset = record.offset();
            }
            if (timestampRecord == null) {
                timestampRecord = record.timestamp();
            }
        }

        String topicoEfectivo = topico != null ? topico : "audit.timeline";
        String particionStr = particion != null ? String.valueOf(particion) : "N/A";
        String offsetStr = offset != null ? String.valueOf(offset) : "N/A";
        String keyStr = key != null ? key : "N/A";

        Long pedidoId = extraerPedidoId(datos, key);
        String tipoEvento = extraerTipoEvento(datos);

        log.info("""
                ====================== TIMELINE CONSUMER ======================
                Tópico:            {}
                Partición:         {}
                Offset:            {}
                Clave (Key):       {}
                Pedido ID:         {}
                Tipo de Evento:    {}
                Timestamp Broker:  {}
                Payload Recibido:  {}
                ===============================================================""",
                topicoEfectivo, particionStr, offsetStr, keyStr,
                pedidoId != null ? pedidoId : "N/A",
                tipoEvento,
                timestampRecord != null ? Instant.ofEpochMilli(timestampRecord).toString() : "N/A",
                datos
        );

        Map<String, Object> registro = new LinkedHashMap<>();
        registro.put("topico", topicoEfectivo);
        registro.put("particion", particion);
        registro.put("offset", offset);
        registro.put("key", key);
        registro.put("tipoEvento", tipoEvento);
        registro.put("pedidoId", pedidoId);
        registro.put("timestampBroker", timestampRecord != null ? Instant.ofEpochMilli(timestampRecord).toString() : null);
        registro.put("registradoEn", Instant.now().toString());

        if (datos instanceof Map<?, ?> mapPayload) {
            mapPayload.forEach((k, v) -> registro.put(String.valueOf(k), v));
        } else if (datos != null) {
            registro.put("datos", datos.toString());
        }

        if (pedidoId != null) {
            ultimoEstadoPorPedido.put(pedidoId, registro);
            historialEventosPorPedido.computeIfAbsent(pedidoId, k -> new CopyOnWriteArrayList<>()).add(registro);
        }
    }

    /**
     * Obtiene el resumen y los eventos registrados para un pedido en particular.
     */
    public Map<String, Object> obtenerTimelinePedido(Long pedidoId) {
        Map<String, Object> timeline = new LinkedHashMap<>();
        timeline.put("pedidoId", pedidoId);

        Map<String, Object> ultimo = ultimoEstadoPorPedido.get(pedidoId);
        List<Map<String, Object>> eventos = historialEventosPorPedido.getOrDefault(pedidoId, Collections.emptyList());

        timeline.put("ultimoEstado", ultimo != null ? ultimo.getOrDefault("tipoEvento", "N/A") : null);
        timeline.put("ultimoRegistro", ultimo);
        timeline.put("totalEventos", eventos.size());
        timeline.put("eventos", eventos);
        timeline.put("timestampConsulta", Instant.now().toString());

        return timeline;
    }

    public Map<Long, Map<String, Object>> obtenerUltimosEstados() {
        return Collections.unmodifiableMap(ultimoEstadoPorPedido);
    }

    public void limpiarParaPruebas() {
        ultimoEstadoPorPedido.clear();
        historialEventosPorPedido.clear();
    }

    private Long extraerPedidoId(Object payload, String key) {
        if (payload instanceof Map<?, ?> map) {
            Object idObj = map.get("pedidoId");
            if (idObj instanceof Number n) {
                return n.longValue();
            } else if (idObj != null) {
                try {
                    return Long.parseLong(idObj.toString().trim());
                } catch (NumberFormatException ignored) {
                }
            }
        }
        if (key != null) {
            try {
                return Long.parseLong(key.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return null;
    }

    private String extraerTipoEvento(Object payload) {
        if (payload instanceof Map<?, ?> map) {
            Object tipo = map.get("tipoEvento");
            if (tipo != null) {
                return tipo.toString();
            }
        }
        return "N/A";
    }
}
