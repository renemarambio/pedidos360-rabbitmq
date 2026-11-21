package cl.duoc.dsy1107.pedidos360.pedidos.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Collections;
import java.util.Map;

/**
 * Solicitud de creación programática de un nuevo tópico Kafka.
 */
public record CrearTopicoRequest(
        @NotBlank(message = "El nombre del tópico es obligatorio")
        String nombre,

        @Min(value = 1, message = "Debe especificar al menos 1 partición")
        Integer particiones,

        @Min(value = 1, message = "El factor de replicación debe ser al menos 1")
        Short replicas,

        Map<String, String> configs
) {
    public int particionesOrDefault() {
        return particiones != null ? particiones : 3;
    }

    public short replicasOrDefault() {
        return replicas != null ? replicas : 3;
    }

    public Map<String, String> configsOrDefault() {
        return configs != null ? configs : Collections.emptyMap();
    }
}
