package cl.duoc.dsy1107.pedidos360.pedidos;

import cl.duoc.dsy1107.pedidos360.pedidos.api.KafkaAdminController;
import cl.duoc.dsy1107.pedidos360.pedidos.api.dto.CrearTopicoRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class KafkaAdminControllerTest {

    private MockMvc mockMvc;

    @Mock
    private AdminClient adminClient;

    private KafkaAdminController controller;

    @BeforeEach
    void setUp() {
        controller = new KafkaAdminController(adminClient);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void listarTopicos_retornaParticionesRFeIsr() throws Exception {
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of("orders.events")));

        Node n1 = new Node(1, "kafka-1", 9092);
        Node n2 = new Node(2, "kafka-2", 9093);
        Node n3 = new Node(3, "kafka-3", 9094);
        TopicPartitionInfo p0 = new TopicPartitionInfo(0, n1, List.of(n1, n2, n3), List.of(n1, n2, n3));
        TopicDescription td = new TopicDescription("orders.events", false, List.of(p0));

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("orders.events", td)));

        mockMvc.perform(get("/api/kafka/admin/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].nombre").value("orders.events"))
                .andExpect(jsonPath("$[0].totalParticiones").value(1))
                .andExpect(jsonPath("$[0].factorReplicacion").value(3))
                .andExpect(jsonPath("$[0].particiones[0].particion").value(0))
                .andExpect(jsonPath("$[0].particiones[0].lider").value(1))
                .andExpect(jsonPath("$[0].particiones[0].replicas[0]").value(1))
                .andExpect(jsonPath("$[0].particiones[0].isr[2]").value(3));
    }

    @Test
    void crearTopico_exitoso_responde201() throws Exception {
        CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        mockMvc.perform(post("/api/kafka/admin/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"nombre":"topico.nuevo","particiones":3,"replicas":3}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.creado").value(true))
                .andExpect(jsonPath("$.nombre").value("topico.nuevo"))
                .andExpect(jsonPath("$.particiones").value(3))
                .andExpect(jsonPath("$.replicas").value(3));
    }

    @Test
    void crearTopico_conflicto_responde409() throws Exception {
        CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);

        KafkaFutureImpl<Void> failedFuture = new KafkaFutureImpl<>();
        failedFuture.completeExceptionally(new TopicExistsException("El tópico topico.repetido ya existe"));
        when(createTopicsResult.all()).thenReturn(failedFuture);

        mockMvc.perform(post("/api/kafka/admin/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"nombre":"topico.repetido","particiones":3,"replicas":3}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409));
    }

    @Test
    void modificarRetencion_exitoso_responde200() throws Exception {
        org.apache.kafka.clients.admin.AlterConfigsResult alterResult =
                mock(org.apache.kafka.clients.admin.AlterConfigsResult.class);
        when(adminClient.incrementalAlterConfigs(any())).thenReturn(alterResult);
        when(alterResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        mockMvc.perform(put("/api/kafka/admin/topics/mi-topico/retention?ms=86400000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.actualizado").value(true))
                .andExpect(jsonPath("$.topico").value("mi-topico"))
                .andExpect(jsonPath("$.nuevoValorMs").value(86400000))
                .andExpect(jsonPath("$.metodo").value("incrementalAlterConfigs"));
    }

    @Test
    void modificarRetencion_invalido_responde400() throws Exception {
        mockMvc.perform(put("/api/kafka/admin/topics/mi-topico/retention?ms=-10"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void eliminarTopico_protegido_responde400YBloquea() throws Exception {
        mockMvc.perform(delete("/api/kafka/admin/topics/orders.events"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.mensaje").value(org.hamcrest.Matchers.containsString("Operación denegada")));

        verify(adminClient, never()).deleteTopics(anyCollection());
    }

    @Test
    void eliminarTopico_noProtegido_exitoso() throws Exception {
        DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
        when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteResult);
        when(deleteResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        mockMvc.perform(delete("/api/kafka/admin/topics/topico-temporal"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eliminado").value(true))
                .andExpect(jsonPath("$.topico").value("topico-temporal"));
    }

    @Test
    void listarGrupos_retornaEstadoYMiembros() throws Exception {
        ListConsumerGroupsResult listGroupsResult = mock(ListConsumerGroupsResult.class);
        when(adminClient.listConsumerGroups()).thenReturn(listGroupsResult);

        ConsumerGroupListing listing = new ConsumerGroupListing("pedidos360-metricas-group", false, Optional.of(ConsumerGroupState.STABLE));
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(listing)));

        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        when(adminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

        MemberDescription member = new MemberDescription(
                "consumer-1", Optional.empty(), "client-1", "/127.0.0.1",
                new MemberAssignment(Set.of(new TopicPartition("orders.events", 0)))
        );
        ConsumerGroupDescription groupDesc = new ConsumerGroupDescription(
                "pedidos360-metricas-group", false, List.of(member), "range",
                ConsumerGroupState.STABLE, new Node(1, "kafka-1", 9092)
        );
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of("pedidos360-metricas-group", groupDesc)));

        mockMvc.perform(get("/api/kafka/admin/groups"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].groupId").value("pedidos360-metricas-group"))
                .andExpect(jsonPath("$[0].estado").value("Stable"))
                .andExpect(jsonPath("$[0].totalMiembros").value(1))
                .andExpect(jsonPath("$[0].miembros[0].memberId").value("consumer-1"))
                .andExpect(jsonPath("$[0].miembros[0].particionesAsignadas[0]").value("orders.events-0"));
    }

    @Test
    void obtenerLagPorGrupo_calculaCommittedEndYLag() throws Exception {
        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        when(adminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

        TopicPartition tp0 = new TopicPartition("orders.events", 0);
        MemberDescription member = new MemberDescription(
                "consumer-1", Optional.empty(), "client-1", "/127.0.0.1",
                new MemberAssignment(Set.of(tp0))
        );
        ConsumerGroupDescription groupDesc = new ConsumerGroupDescription(
                "pedidos360-metricas-group", false, List.of(member), "range",
                ConsumerGroupState.STABLE, new Node(1, "kafka-1", 9092)
        );
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of("pedidos360-metricas-group", groupDesc)));

        ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
        when(adminClient.listConsumerGroupOffsets("pedidos360-metricas-group")).thenReturn(offsetsResult);
        when(offsetsResult.partitionsToOffsetAndMetadata()).thenReturn(
                KafkaFuture.completedFuture(Map.of(tp0, new OffsetAndMetadata(100L)))
        );

        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        when(adminClient.listOffsets(any())).thenReturn(listOffsetsResult);
        ListOffsetsResult.ListOffsetsResultInfo info0 =
                new ListOffsetsResult.ListOffsetsResultInfo(105L, 1700000000000L, Optional.empty());
        when(listOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(tp0, info0)));

        mockMvc.perform(get("/api/kafka/admin/groups/pedidos360-metricas-group/lag"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.groupId").value("pedidos360-metricas-group"))
                .andExpect(jsonPath("$.totalLag").value(5))
                .andExpect(jsonPath("$.particiones[0].topico").value("orders.events"))
                .andExpect(jsonPath("$.particiones[0].particion").value(0))
                .andExpect(jsonPath("$.particiones[0].committedOffset").value(100))
                .andExpect(jsonPath("$.particiones[0].endOffset").value(105))
                .andExpect(jsonPath("$.particiones[0].lag").value(5));
    }
}
