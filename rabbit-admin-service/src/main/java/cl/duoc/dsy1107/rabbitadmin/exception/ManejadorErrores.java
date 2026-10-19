package cl.duoc.dsy1107.rabbitadmin.exception;

import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.RestClientException;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Convierte cada error en una respuesta JSON clara:
 * {"timestamp", "status", "error", "mensaje", "errores": {campo: motivo}}.
 */
@RestControllerAdvice
public class ManejadorErrores {

    private static final Logger log = LoggerFactory.getLogger(ManejadorErrores.class);

    /** Cuerpo JSON que no cumple las validaciones del DTO. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validacionCuerpo(MethodArgumentNotValidException e) {
        Map<String, String> errores = new LinkedHashMap<>();
        for (FieldError campo : e.getBindingResult().getFieldErrors()) {
            errores.putIfAbsent(campo.getField(), campo.getDefaultMessage());
        }
        e.getBindingResult().getGlobalErrors()
                .forEach(global -> errores.putIfAbsent(global.getObjectName(), global.getDefaultMessage()));
        return respuesta(HttpStatus.BAD_REQUEST, "Parámetros de entrada inválidos", errores);
    }

    /** Parámetros de ruta o query inválidos (ej. nombre de cola con caracteres no permitidos). */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Map<String, Object>> validacionParametros(ConstraintViolationException e) {
        Map<String, String> errores = new LinkedHashMap<>();
        e.getConstraintViolations().forEach(v -> {
            String ruta = v.getPropertyPath().toString();
            errores.putIfAbsent(ruta.substring(ruta.lastIndexOf('.') + 1), v.getMessage());
        });
        return respuesta(HttpStatus.BAD_REQUEST, "Parámetros de entrada inválidos", errores);
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<Map<String, Object>> validacionMetodo(HandlerMethodValidationException e) {
        Map<String, String> errores = new LinkedHashMap<>();
        e.getAllValidationResults().forEach(resultado -> resultado.getResolvableErrors().forEach(error -> {
            String campo = error instanceof FieldError fe
                    ? fe.getField()
                    : resultado.getMethodParameter().getParameterName();
            errores.putIfAbsent(campo, error.getDefaultMessage());
        }));
        return respuesta(HttpStatus.BAD_REQUEST, "Parámetros de entrada inválidos", errores);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> jsonIlegible(HttpMessageNotReadableException e) {
        return respuesta(HttpStatus.BAD_REQUEST,
                "El cuerpo no es un JSON válido o tiene un valor no permitido (revise 'tipo' y los números)", Map.of());
    }

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<Map<String, Object>> noEncontrado(RecursoNoEncontradoException e) {
        return respuesta(HttpStatus.NOT_FOUND, e.getMessage(), Map.of());
    }

    @ExceptionHandler(ConflictoException.class)
    public ResponseEntity<Map<String, Object>> conflicto(ConflictoException e) {
        return respuesta(HttpStatus.CONFLICT, e.getMessage(), Map.of());
    }

    @ExceptionHandler(OperacionNoPermitidaException.class)
    public ResponseEntity<Map<String, Object>> noPermitida(OperacionNoPermitidaException e) {
        return respuesta(HttpStatus.FORBIDDEN, e.getMessage(), Map.of());
    }

    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<Map<String, Object>> brokerNoDisponible(AmqpException e) {
        log.error("Error comunicándose con RabbitMQ: {}", e.getMessage());
        return respuesta(HttpStatus.SERVICE_UNAVAILABLE, "RabbitMQ no está disponible o rechazó la operación", Map.of());
    }

    @ExceptionHandler(RestClientException.class)
    public ResponseEntity<Map<String, Object>> managementNoDisponible(RestClientException e) {
        log.error("Error comunicándose con RabbitMQ Management: {}", e.getMessage());
        return respuesta(HttpStatus.BAD_GATEWAY, "La API de RabbitMQ Management no respondió correctamente", Map.of());
    }

    private ResponseEntity<Map<String, Object>> respuesta(HttpStatus status, String mensaje, Map<String, String> errores) {
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("timestamp", LocalDateTime.now().toString());
        cuerpo.put("status", status.value());
        cuerpo.put("error", status.getReasonPhrase());
        cuerpo.put("mensaje", mensaje);
        if (!errores.isEmpty()) {
            cuerpo.put("errores", errores);
        }
        return ResponseEntity.status(status).body(cuerpo);
    }
}
