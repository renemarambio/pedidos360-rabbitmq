package cl.duoc.dsy1107.rabbitadmin.exception;

/** La cola, exchange, binding o política solicitada no existe (HTTP 404). */
public class RecursoNoEncontradoException extends RuntimeException {
    public RecursoNoEncontradoException(String mensaje) {
        super(mensaje);
    }
}
