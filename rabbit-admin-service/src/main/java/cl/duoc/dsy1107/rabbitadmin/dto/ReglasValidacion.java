package cl.duoc.dsy1107.rabbitadmin.dto;

/** Reglas de formato compartidas por los DTO y los parámetros de ruta. */
public final class ReglasValidacion {

    /** 3 a 100 caracteres: letras, números, punto, guion o guion bajo; parte con letra o número. */
    public static final String NOMBRE = "^[a-zA-Z0-9][a-zA-Z0-9._-]{2,99}$";
    public static final String MENSAJE_NOMBRE =
            "debe tener entre 3 y 100 caracteres (letras, números, '.', '-' o '_') y comenzar con letra o número";

    /** Routing key: palabras separadas por punto; admite comodines * y # para topic exchanges. Puede ser vacía. */
    public static final String ROUTING_KEY = "^[a-zA-Z0-9_*#-]*(\\.[a-zA-Z0-9_*#-]+)*$";
    public static final String MENSAJE_ROUTING_KEY =
            "solo letras, números, '_', '-', '*', '#' separados por '.' (ej. pedidos.*.confirmado)";

    /** Máximos razonables para evitar configuraciones absurdas. */
    public static final long TTL_MAXIMO_MS = 604_800_000L;      // 7 días
    public static final long LARGO_MAXIMO_COLA = 1_000_000L;

    private ReglasValidacion() {
    }
}
