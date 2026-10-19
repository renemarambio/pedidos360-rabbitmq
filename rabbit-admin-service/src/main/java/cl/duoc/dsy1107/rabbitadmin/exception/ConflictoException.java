package cl.duoc.dsy1107.rabbitadmin.exception;

/** El recurso ya existe o su estado impide la operación (HTTP 409). */
public class ConflictoException extends RuntimeException {
    public ConflictoException(String mensaje) {
        super(mensaje);
    }
}
