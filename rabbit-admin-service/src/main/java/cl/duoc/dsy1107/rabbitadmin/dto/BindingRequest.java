package cl.duoc.dsy1107.rabbitadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Enlace entre un exchange y una cola (POST y DELETE /api/admin/bindings).
 * Ejemplo: {"exchange":"demo.pagos.topic","cola":"demo.pagos.queue","routingKey":"pagos.*.confirmado"}
 */
public record BindingRequest(
        @Schema(example = "demo.pagos.topic")
        @NotBlank(message = "el exchange es obligatorio")
        @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
        String exchange,

        @Schema(example = "demo.pagos.queue")
        @NotBlank(message = "la cola es obligatoria")
        @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
        String cola,

        @Schema(description = "Use \"\" para fanout", example = "pagos.*.confirmado")
        @NotNull(message = "la routing key es obligatoria (use \"\" para fanout)")
        @Size(max = 255, message = "la routing key no puede superar 255 caracteres")
        @Pattern(regexp = ReglasValidacion.ROUTING_KEY, message = ReglasValidacion.MENSAJE_ROUTING_KEY)
        String routingKey
) {
}
