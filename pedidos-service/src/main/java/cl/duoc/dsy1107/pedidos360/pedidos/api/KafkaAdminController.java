package cl.duoc.dsy1107.pedidos360.pedidos.api;

import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.CrearTopicoRequest;
import jakarta.validation.Valid;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Controlador REST para administración programática de Apache Kafka mediante AdminClient.
 *
 * Expone operaciones de inspección, creación, retención y monitoreo de lag
 * sobre tópicos y consumer groups del clúster Kafka.
 */
@RestController
@RequestMapping("/api/kafka/admin")
public class KafkaAdminController {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminController.class);

    // Tópicos protegidos del sistema que no pueden ser eliminados
    private static final Set<String> TOPICOS_SISTEMA = Set.of(
            "orders.events",
            "audit.timeline",
            "orders.events.DLT"
    );

    private final AdminClient adminClient;

    public KafkaAdminController(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /**
     * Lista todos los tópicos del clúster con sus particiones, factor de replicación, líder e ISR.
     */
    @GetMapping("/topics")
    public ResponseEntity<?> listarTopicos() {
        try {
            Set<String> nombres = adminClient.listTopics().names().get();
            if (nombres.isEmpty()) {
                return ResponseEntity.ok(Collections.emptyList());
            }

            Map<String, TopicDescription> descripciones = adminClient.describeTopics(nombres).allTopicNames().get();
            List<Map<String, Object>> resultado = new ArrayList<>();

            for (TopicDescription desc : descripciones.values()) {
                Map<String, Object> topicInfo = new LinkedHashMap<>();
                topicInfo.put("nombre", desc.name());
                topicInfo.put("internal", desc.isInternal());
                topicInfo.put("totalParticiones", desc.partitions().size());

                int rf = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
                topicInfo.put("factorReplicacion", rf);

                List<Map<String, Object>> particiones = desc.partitions().stream().map(p -> {
                    Map<String, Object> pInfo = new LinkedHashMap<>();
                    pInfo.put("particion", p.partition());
                    pInfo.put("lider", p.leader() != null ? p.leader().id() : -1);
                    pInfo.put("replicas", p.replicas().stream().map(Node::id).toList());
                    pInfo.put("isr", p.isr().stream().map(Node::id).toList());
                    return pInfo;
                }).toList();

                topicInfo.put("particiones", particiones);
                resultado.add(topicInfo);
            }

            resultado.sort(Comparator.comparing(t -> (String) t.get("nombre")));
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            return manejarExcepcion(e, "Error al listar tópicos de Kafka");
        }
    }

    /**
     * Crea un nuevo tópico Kafka indicando nombre, particiones y réplicas.
     */
    @PostMapping("/topics")
    public ResponseEntity<?> crearTopico(@Valid @RequestBody CrearTopicoRequest request) {
        if (request.nombre() == null || request.nombre().trim().isEmpty()) {
            return error(HttpStatus.BAD_REQUEST, "El nombre del tópico es obligatorio");
        }
        if (request.particiones() != null && request.particiones() < 1) {
            return error(HttpStatus.BAD_REQUEST, "Debe tener al menos 1 partición");
        }
        if (request.replicas() != null && request.replicas() < 1) {
            return error(HttpStatus.BAD_REQUEST, "Debe tener al menos 1 réplica");
        }

        try {
            NewTopic nuevoTopico = new NewTopic(
                    request.nombre().trim(),
                    request.particionesOrDefault(),
                    request.replicasOrDefault()
            ).configs(request.configsOrDefault());

            adminClient.createTopics(Collections.singleton(nuevoTopico)).all().get();

            Map<String, Object> respuesta = new LinkedHashMap<>();
            respuesta.put("creado", true);
            respuesta.put("nombre", request.nombre().trim());
            respuesta.put("particiones", request.particionesOrDefault());
            respuesta.put("replicas", request.replicasOrDefault());
            respuesta.put("mensaje", "Tópico creado exitosamente");
            return ResponseEntity.status(HttpStatus.CREATED).body(respuesta);
        } catch (Exception e) {
            return manejarExcepcion(e, "Error al crear tópico: " + request.nombre());
        }
    }

    /**
     * Modifica la retención en milisegundos (retention.ms) usando incrementalAlterConfigs.
     */
    @PutMapping("/topics/{nombre}/retention")
    public ResponseEntity<?> modificarRetencion(
            @PathVariable("nombre") String nombre,
            @RequestParam("ms") Long ms
    ) {
        if (ms == null || ms <= 0) {
            return error(HttpStatus.BAD_REQUEST, "El parámetro 'ms' debe ser un número entero mayor a 0");
        }

        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            AlterConfigOp op = new AlterConfigOp(
                    new ConfigEntry("retention.ms", String.valueOf(ms)),
                    AlterConfigOp.OpType.SET
            );

            adminClient.incrementalAlterConfigs(Map.of(resource, List.of(op))).all().get();

            Map<String, Object> respuesta = new LinkedHashMap<>();
            respuesta.put("actualizado", true);
            respuesta.put("topico", nombre);
            respuesta.put("configuracion", "retention.ms");
            respuesta.put("nuevoValorMs", ms);
            respuesta.put("metodo", "incrementalAlterConfigs");
            respuesta.put("mensaje", "Política retention.ms actualizada exitosamente");
            return ResponseEntity.ok(respuesta);
        } catch (Exception e) {
            return manejarExcepcion(e, "Error al actualizar retención para tópico: " + nombre);
        }
    }

    /**
     * Elimina un tópico asegurando el bloqueo de los 3 tópicos del sistema.
     */
    @DeleteMapping("/topics/{nombre}")
    public ResponseEntity<?> eliminarTopico(@PathVariable("nombre") String nombre) {
        if (TOPICOS_SISTEMA.contains(nombre)) {
            log.warn("[KAFKA-ADMIN] Intento bloqueado de eliminar tópico protegido del sistema: {}", nombre);
            return error(HttpStatus.BAD_REQUEST,
                    "Operación denegada: no está permitido eliminar los tópicos base del sistema: " + TOPICOS_SISTEMA);
        }

        try {
            adminClient.deleteTopics(Collections.singleton(nombre)).all().get();

            Map<String, Object> respuesta = new LinkedHashMap<>();
            respuesta.put("eliminado", true);
            respuesta.put("topico", nombre);
            respuesta.put("mensaje", "Tópico eliminado exitosamente");
            return ResponseEntity.ok(respuesta);
        } catch (Exception e) {
            return manejarExcepcion(e, "Error al eliminar tópico: " + nombre);
        }
    }

    /**
     * Lista todos los consumer groups con su estado y miembros.
     */
    @GetMapping("/groups")
    public ResponseEntity<?> listarGrupos() {
        try {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups().all().get();
            if (listings.isEmpty()) {
                return ResponseEntity.ok(Collections.emptyList());
            }

            List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).toList();
            Map<String, ConsumerGroupDescription> descriptions = adminClient.describeConsumerGroups(groupIds).all().get();

            List<Map<String, Object>> resultado = new ArrayList<>();
            for (ConsumerGroupListing listing : listings) {
                ConsumerGroupDescription desc = descriptions.get(listing.groupId());
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("groupId", listing.groupId());
                info.put("isSimpleConsumerGroup", listing.isSimpleConsumerGroup());
                info.put("estado", desc != null && desc.state() != null
                        ? desc.state().toString()
                        : (listing.state().isPresent() ? listing.state().get().toString() : "UNKNOWN"));

                if (desc != null) {
                    info.put("partitionAssignor", desc.partitionAssignor());
                    info.put("coordinador", desc.coordinator() != null
                            ? desc.coordinator().id() + " (" + desc.coordinator().host() + ":" + desc.coordinator().port() + ")"
                            : "N/A");

                    List<Map<String, Object>> miembros = desc.members().stream().map(m -> {
                        Map<String, Object> mInfo = new LinkedHashMap<>();
                        mInfo.put("memberId", m.consumerId());
                        mInfo.put("clientId", m.clientId());
                        mInfo.put("host", m.host());
                        List<String> particiones = m.assignment().topicPartitions().stream()
                                .map(tp -> tp.topic() + "-" + tp.partition())
                                .sorted()
                                .toList();
                        mInfo.put("particionesAsignadas", particiones);
                        return mInfo;
                    }).toList();

                    info.put("miembros", miembros);
                    info.put("totalMiembros", miembros.size());
                }
                resultado.add(info);
            }

            resultado.sort(Comparator.comparing(g -> (String) g.get("groupId")));
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            return manejarExcepcion(e, "Error al listar consumer groups de Kafka");
        }
    }

    /**
     * Calcula y reporta el lag por partición (committed offset, end offset, lag = end - committed).
     */
    @GetMapping("/groups/{groupId}/lag")
    public ResponseEntity<?> obtenerLagPorGrupo(@PathVariable("groupId") String groupId) {
        try {
            ConsumerGroupDescription groupDesc;
            try {
                Map<String, ConsumerGroupDescription> descMap =
                        adminClient.describeConsumerGroups(Collections.singleton(groupId)).all().get();
                groupDesc = descMap.get(groupId);
            } catch (Exception e) {
                return error(HttpStatus.NOT_FOUND, "Consumer group no encontrado: " + groupId);
            }

            if (groupDesc == null) {
                return error(HttpStatus.NOT_FOUND, "Consumer group no encontrado: " + groupId);
            }

            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

            Set<TopicPartition> allPartitions = new HashSet<>(committedOffsets.keySet());
            if (groupDesc.members() != null) {
                for (MemberDescription member : groupDesc.members()) {
                    if (member.assignment() != null && member.assignment().topicPartitions() != null) {
                        allPartitions.addAll(member.assignment().topicPartitions());
                    }
                }
            }

            if (allPartitions.isEmpty()) {
                Map<String, Object> vacio = new LinkedHashMap<>();
                vacio.put("groupId", groupId);
                vacio.put("estado", groupDesc.state() != null ? groupDesc.state().toString() : "UNKNOWN");
                vacio.put("totalLag", 0L);
                vacio.put("particiones", Collections.emptyList());
                vacio.put("mensaje", "El grupo no tiene particiones asignadas ni offsets comprometidos aún");
                vacio.put("timestamp", Instant.now().toString());
                return ResponseEntity.ok(vacio);
            }

            Map<TopicPartition, OffsetSpec> requestOffsets = allPartitions.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    adminClient.listOffsets(requestOffsets).all().get();

            long totalLag = 0L;
            List<Map<String, Object>> particionesLag = new ArrayList<>();
            List<TopicPartition> sortedPartitions = allPartitions.stream()
                    .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                    .toList();

            for (TopicPartition tp : sortedPartitions) {
                long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : 0L;
                OffsetAndMetadata committed = committedOffsets.get(tp);
                long committedOffset = committed != null ? committed.offset() : 0L;
                long lag = Math.max(0L, endOffset - committedOffset);
                totalLag += lag;

                Map<String, Object> pInfo = new LinkedHashMap<>();
                pInfo.put("topico", tp.topic());
                pInfo.put("particion", tp.partition());
                pInfo.put("committedOffset", committedOffset);
                pInfo.put("endOffset", endOffset);
                pInfo.put("lag", lag);
                particionesLag.add(pInfo);
            }

            Map<String, Object> resultado = new LinkedHashMap<>();
            resultado.put("groupId", groupId);
            resultado.put("estado", groupDesc.state() != null ? groupDesc.state().toString() : "UNKNOWN");
            resultado.put("totalLag", totalLag);
            resultado.put("particiones", particionesLag);
            resultado.put("timestamp", Instant.now().toString());
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            return manejarExcepcion(e, "Error al calcular lag para el grupo: " + groupId);
        }
    }

    private ResponseEntity<Map<String, Object>> manejarExcepcion(Exception e, String mensajeContexto) {
        Throwable causa = e instanceof ExecutionException ? e.getCause() : e;
        log.error("[KAFKA-ADMIN] {}: {}", mensajeContexto, causa.getMessage());

        if (causa instanceof TopicExistsException) {
            return error(HttpStatus.CONFLICT, "Conflicto: el tópico ya existe (" + causa.getMessage() + ")");
        }
        if (causa instanceof UnknownTopicOrPartitionException) {
            return error(HttpStatus.NOT_FOUND, "Recurso no encontrado: tópico o partición inexistente");
        }
        if (causa instanceof GroupIdNotFoundException) {
            return error(HttpStatus.NOT_FOUND, "Consumer group no encontrado en el clúster");
        }
        if (causa instanceof InvalidPartitionsException || causa instanceof InvalidReplicationFactorException
                || causa instanceof IllegalArgumentException) {
            return error(HttpStatus.BAD_REQUEST, "Solicitud inválida: " + causa.getMessage());
        }

        return error(HttpStatus.INTERNAL_SERVER_ERROR, mensajeContexto + ": " + causa.getMessage());
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String mensaje) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("timestamp", Instant.now().toString());
        err.put("status", status.value());
        err.put("error", status.getReasonPhrase());
        err.put("mensaje", mensaje);
        return ResponseEntity.status(status).body(err);
    }
}
