package cl.duoc.dsy1107.pedidos360.pedidos.api.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

/**
 * Pedido enviado por el catálogo web (POST /api/pedidos).
 * Los campos falloX son opcionales y sirven para simular errores en un dominio.
 */
public record CrearPedidoRequest(
        @Pattern(regexp = "^PED-\\d{1,9}$", message = "debe tener el formato PED-1234")
        String idPedido,

        @Size(max = 50, message = "no puede superar 50 caracteres")
        String productoId,

        @NotBlank(message = "es obligatorio")
        @Size(max = 120, message = "no puede superar 120 caracteres")
        String nombreProducto,

        @NotNull(message = "es obligatorio")
        @DecimalMin(value = "1", message = "debe ser mayor a 0")
        BigDecimal precio,

        @NotNull(message = "es obligatoria")
        @Min(value = 1, message = "debe ser al menos 1")
        @Max(value = 100, message = "no puede superar 100")
        Integer cantidad,

        String fecha,
        boolean falloNotificaciones,
        boolean falloCocina,
        boolean falloDocumentos
) {
}
