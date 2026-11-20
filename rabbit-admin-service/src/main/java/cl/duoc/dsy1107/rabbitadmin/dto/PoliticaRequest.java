package cl.duoc.dsy1107.rabbitadmin.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.regex.PatternSyntaxException;

/**
 * Política de retención/limpieza aplicada por patrón (PUT /api/admin/policies/{nombre}).
 * Ejemplo: {"patron":"^pedidos360\\.monitoreo\\.","ttlMs":3600000,"maxLength":500}
 */
public record PoliticaRequest(
        @Schema(description = "Expresión regular sobre el nombre de las colas", example = "^pedidos360\\.monitoreo\\.")
        @NotBlank(message = "el patrón es obligatorio")
        @Size(max = 200, message = "el patrón no puede superar 200 caracteres")
        String patron,

        @Schema(description = "Tiempo de vida de los mensajes en milisegundos", example = "3600000")
        @Positive(message = "el TTL debe ser mayor a 0")
        @Max(value = ReglasValidacion.TTL_MAXIMO_MS, message = "el TTL no puede superar 7 días (604800000 ms)")
        Long ttlMs,

        @Schema(description = "Cantidad máxima de mensajes por cola", example = "500")
        @Positive(message = "el largo máximo debe ser mayor a 0")
        @Max(value = ReglasValidacion.LARGO_MAXIMO_COLA, message = "el largo máximo no puede superar 1.000.000")
        Long maxLength,

        @Schema(description = "Prioridad frente a otras políticas", defaultValue = "0")
        @Min(value = 0, message = "la prioridad mínima es 0")
        @Max(value = 999, message = "la prioridad máxima es 999")
        Integer prioridad
) {

    @Schema(hidden = true)
    @AssertTrue(message = "indique al menos ttlMs o maxLength")
    public boolean isDefinicionPresente() {
        return ttlMs != null || maxLength != null;
    }

    @Schema(hidden = true)
    @AssertTrue(message = "el patrón no es una expresión regular válida")
    public boolean isPatronValido() {
        if (patron == null || patron.isBlank()) {
            return true; // lo informa @NotBlank
        }
        try {
            java.util.regex.Pattern.compile(patron);
            return true;
        } catch (PatternSyntaxException e) {
            return false;
        }
    }
}
