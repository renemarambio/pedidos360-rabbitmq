package cl.duoc.dsy1107.rabbitadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

/**
 * Datos para crear una cola (POST /api/admin/queues).
 * Ejemplo: {"nombre":"demo.pagos.queue","ttlMs":60000,"maxLength":500,
 *           "deadLetterExchange":"pedidos360.dlx","deadLetterRoutingKey":"pagos.failed"}
 */
public record CrearColaRequest(
        @Schema(example = "demo.pagos.queue")
        @NotBlank(message = "el nombre de la cola es obligatorio")
        @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
        String nombre,

        @Schema(description = "Sobrevive a reinicios del broker", defaultValue = "true")
        Boolean durable,

        @Schema(description = "Se elimina sola cuando se desconecta el último consumidor", defaultValue = "false")
        Boolean autoDelete,

        @Schema(description = "Tiempo de vida de cada mensaje en milisegundos", example = "60000")
        @Positive(message = "el TTL debe ser mayor a 0")
        @Max(value = ReglasValidacion.TTL_MAXIMO_MS, message = "el TTL no puede superar 7 días (604800000 ms)")
        Long ttlMs,

        @Schema(description = "Cantidad máxima de mensajes en la cola", example = "500")
        @Positive(message = "el largo máximo debe ser mayor a 0")
        @Max(value = ReglasValidacion.LARGO_MAXIMO_COLA, message = "el largo máximo no puede superar 1.000.000")
        Long maxLength,

        @Schema(description = "Exchange al que se envían los mensajes rechazados o expirados", example = "pedidos360.dlx")
        @Pattern(regexp = ReglasValidacion.NOMBRE, message = ReglasValidacion.MENSAJE_NOMBRE)
        String deadLetterExchange,

        @Schema(example = "pagos.failed")
        @Pattern(regexp = ReglasValidacion.ROUTING_KEY, message = ReglasValidacion.MENSAJE_ROUTING_KEY)
        String deadLetterRoutingKey
) {

    public boolean esDurable() {
        return durable == null || durable;
    }

    public boolean esAutoDelete() {
        return autoDelete != null && autoDelete;
    }

    /** Una routing key de dead letter no tiene sentido sin su exchange. */
    @Schema(hidden = true)
    @AssertTrue(message = "deadLetterRoutingKey requiere indicar deadLetterExchange")
    public boolean isDeadLetterConsistente() {
        return deadLetterRoutingKey == null || deadLetterRoutingKey.isBlank()
                || (deadLetterExchange != null && !deadLetterExchange.isBlank());
    }

    /** Una cola durable no puede autoeliminarse: se rechaza la combinación contradictoria. */
    @Schema(hidden = true)
    @AssertTrue(message = "una cola no puede ser durable y autoDelete a la vez")
    public boolean isDurabilidadConsistente() {
        return !(esDurable() && esAutoDelete());
    }
}
