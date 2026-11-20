package cl.duoc.dsy1107.rabbitadmin.dto;

/** Nodo del clúster RabbitMQ. */
public record NodoInfo(String nombre, boolean activo, String tipo) {
}
