package cl.duoc.dsy1107.pedidos360.mensajeria.consumo;

/**
 * Error que lanza un servicio de dominio al procesar un evento.
 *
 * - recuperable = true: falla temporal (servicio externo caído, timeout).
 *   El mensaje se reintenta hasta agotar los intentos y luego va a la DLQ.
 * - recuperable = false: el mensaje nunca podrá procesarse (datos inválidos).
 *   Va directo a la DLQ, sin reintentos.
 */
public class ErrorProcesamientoException extends RuntimeException {

    private final boolean recuperable;

    private ErrorProcesamientoException(String mensaje, boolean recuperable) {
        super(mensaje);
        this.recuperable = recuperable;
    }

    public static ErrorProcesamientoException recuperable(String mensaje) {
        return new ErrorProcesamientoException(mensaje, true);
    }

    public static ErrorProcesamientoException noRecuperable(String mensaje) {
        return new ErrorProcesamientoException(mensaje, false);
    }

    public boolean isRecuperable() {
        return recuperable;
    }
}
