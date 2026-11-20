package cl.duoc.dsy1107.rabbitadmin.dto;

/** Exchange existente en el broker. */
public record ExchangeInfo(String nombre, String tipo, boolean durable) {
}
