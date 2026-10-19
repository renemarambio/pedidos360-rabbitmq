package cl.duoc.dsy1107.rabbitadmin.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

/** Configuración del administrador leída desde application.yml (prefijo rabbit-admin). */
@Validated
@ConfigurationProperties(prefix = "rabbit-admin")
public record AdminProperties(
        @Valid @NotNull Management management,
        List<String> prefijosProtegidos
) {

    public AdminProperties {
        prefijosProtegidos = prefijosProtegidos == null ? List.of() : List.copyOf(prefijosProtegidos);
    }

    /** Conexión a la API HTTP de RabbitMQ Management. */
    public record Management(
            @NotBlank String url,
            @NotBlank String usuario,
            @NotBlank String password,
            @NotBlank String vhost
    ) {
    }
}
