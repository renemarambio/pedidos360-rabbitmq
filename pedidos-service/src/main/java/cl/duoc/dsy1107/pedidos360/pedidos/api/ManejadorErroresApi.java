package cl.duoc.dsy1107.pedidos360.pedidos.api;

import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/** Respuestas de error uniformes y legibles para la API de pedidos. */
@RestControllerAdvice
public class ManejadorErroresApi {

    private static final Logger log = LoggerFactory.getLogger(ManejadorErroresApi.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> cuerpoInvalido(MethodArgumentNotValidException e) {
        Map<String, String> campos = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors()
                .forEach(error -> campos.putIfAbsent(error.getField(), error.getDefaultMessage()));
        return error(HttpStatus.BAD_REQUEST, "Datos del pedido inválidos", campos);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Map<String, Object>> parametroInvalido(ConstraintViolationException e) {
        Map<String, String> campos = new LinkedHashMap<>();
        e.getConstraintViolations().forEach(v -> {
            String ruta = v.getPropertyPath().toString();
            campos.putIfAbsent(ruta.substring(ruta.lastIndexOf('.') + 1), v.getMessage());
        });
        return error(HttpStatus.BAD_REQUEST, "Parámetros inválidos", campos);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> jsonIlegible(HttpMessageNotReadableException e) {
        return error(HttpStatus.BAD_REQUEST, "El cuerpo no es un JSON válido", Map.of());
    }

    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<Map<String, Object>> brokerNoDisponible(AmqpException e) {
        log.error("No se pudo publicar en RabbitMQ: {}", e.getMessage());
        return error(HttpStatus.SERVICE_UNAVAILABLE, "RabbitMQ no está disponible, intente nuevamente", Map.of());
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String mensaje, Map<String, String> campos) {
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("timestamp", LocalDateTime.now().toString());
        cuerpo.put("status", status.value());
        cuerpo.put("mensaje", mensaje);
        if (!campos.isEmpty()) {
            cuerpo.put("errores", campos);
        }
        return ResponseEntity.status(status).body(cuerpo);
    }
}
