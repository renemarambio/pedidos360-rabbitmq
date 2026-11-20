package cl.duoc.dsy1107.rabbitadmin.exception;

/** Operación sobre un recurso protegido o reservado por RabbitMQ (HTTP 403). */
public class OperacionNoPermitidaException extends RuntimeException {
    public OperacionNoPermitidaException(String mensaje) {
        super(mensaje);
    }
}
