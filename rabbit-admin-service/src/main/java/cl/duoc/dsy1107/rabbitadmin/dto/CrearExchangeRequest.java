package cl.duoc.dsy1107.rabbitadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/**
 * Datos para crear un exchange (POST /api/admin/exchanges).
 * Ejemplo: {"nombre":"demo.pagos.topic","tipo":"TOPIC"}
 */
public record CrearExchangeRequest(
        @Schema(example = "demo.pagos.topic")
        @NotBlank(message = "el nombre del exchange es obligatorio")
        @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
        String nombre,

        @Schema(example = "TOPIC")
        @NotNull(message = "el tipo es obligatorio: DIRECT, TOPIC, FANOUT o HEADERS")
        TipoExchange tipo,

        @Schema(defaultValue = "true")
        Boolean durable
) {

    public boolean esDurable() {
        return durable == null || durable;
    }
}
