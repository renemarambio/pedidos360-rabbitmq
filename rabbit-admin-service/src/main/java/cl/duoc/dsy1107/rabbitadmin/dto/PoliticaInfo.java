package cl.duoc.dsy1107.rabbitadmin.dto;

import java.util.Map;

/** Política configurada en el broker. */
public record PoliticaInfo(String nombre, String patron, String aplicaA, int prioridad, Map<String, Object> definicion) {
}
