package cl.duoc.dsy1107.pedidos360.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Nombres de exchanges, colas y routing keys leídos desde application.yml
 * (prefijo {@code pedidos360.rabbit}).
 *
 * Es el único punto donde vive la topología: las clases de configuración,
 * el publicador y los consumidores la obtienen desde aquí.
 * Si falta un valor, la aplicación no arranca (validación al inicio).
 */
@Validated
@ConfigurationProperties(prefix = "pedidos360.rabbit")
public record MensajeriaProperties(
        @Valid @NotNull Exchanges exchanges,
        @Valid @NotNull Reintentos reintentos,
        @Valid @NotNull RetencionDlq retencionDlq,
        @Valid @NotNull Dominio notificaciones,
        @Valid @NotNull Dominio cocina,
        @Valid @NotNull Dominio documentos,
        @Valid @NotNull Monitoreo monitoreo,
        @Valid @NotNull AuditoriaDlq auditoriaDlq
) {

    /** Exchanges del sistema. */
    public record Exchanges(
            @NotBlank String direct,
            @NotBlank String topic,
            @NotBlank String deadLetter
    ) {
    }

    /** Política de reintentos para errores recuperables. */
    public record Reintentos(
            @Min(0) int maxIntentos,
            @Min(1) long ttlMs,
            @NotBlank String headerIntentos
    ) {
    }

    /** Política de retención de las DLQ: tiempo de vida y largo máximo. */
    public record RetencionDlq(
            @Min(1) long ttlMs,
            @Min(1) int maxLength
    ) {
    }

    /**
     * Ruta de mensajería completa de un dominio:
     * cola principal, cola de reintento con TTL y cola de mensajes muertos (DLQ).
     */
    public record Dominio(
            @NotBlank String nombre,
            @NotBlank String queue,
            @NotBlank String retryQueue,
            @NotBlank String dlq,
            @NotBlank String routingKey,
            @NotBlank String retryRoutingKey,
            @NotBlank String deadLetterRoutingKey
    ) {
    }

    /** Cola de observabilidad conectada al topic exchange. */
    public record Monitoreo(
            @NotBlank String queue,
            @NotBlank String bindingPattern,
            @NotBlank String routingKeyPorDefecto
    ) {
    }

    /** Activa o desactiva el consumidor que registra en el log los mensajes de las DLQ. */
    public record AuditoriaDlq(boolean habilitada) {
    }
}
