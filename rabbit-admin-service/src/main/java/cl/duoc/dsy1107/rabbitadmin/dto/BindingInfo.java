package cl.duoc.dsy1107.rabbitadmin.dto;

/** Binding existente entre un exchange y un destino. */
public record BindingInfo(String exchange, String destino, String tipoDestino, String routingKey) {
}
