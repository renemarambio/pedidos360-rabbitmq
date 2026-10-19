package cl.duoc.dsy1107.pedidos360.pedidos.api.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * Pedido de laboratorio (POST /api/lab/pedidos/aceptar).
 * Todos los campos son opcionales: si faltan se completan con valores de prueba.
 */
public record PedidoSolicitud(
        @Positive(message = "debe ser positivo")
        Long pedidoId,

        @Email(message = "debe ser un correo válido")
        String clienteEmail,

        @DecimalMin(value = "1", message = "debe ser mayor a 0")
        BigDecimal total,

        boolean falloNotificaciones,
        boolean falloCocina,
        boolean falloDocumentos
) {
}
