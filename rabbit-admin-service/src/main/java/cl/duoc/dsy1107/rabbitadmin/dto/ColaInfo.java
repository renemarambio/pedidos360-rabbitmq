package cl.duoc.dsy1107.rabbitadmin.dto;

import java.util.Map;

/** Estado de una cola con las métricas que muestra la Management UI. */
public record ColaInfo(
        String nombre,
        long mensajesListos,
        long mensajesSinConfirmar,
        long mensajesTotales,
        int consumidores,
        boolean durable,
        String nodo,
        String politica,
        Map<String, Object> argumentos
) {
}
