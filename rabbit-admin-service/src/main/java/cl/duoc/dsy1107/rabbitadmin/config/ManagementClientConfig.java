package cl.duoc.dsy1107.rabbitadmin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Cliente HTTP preconfigurado para la API de RabbitMQ Management. */
@Configuration
public class ManagementClientConfig {

    @Bean
    public RestClient rabbitManagementRestClient(RestClient.Builder builder, AdminProperties props) {
        AdminProperties.Management m = props.management();
        String credenciales = Base64.getEncoder()
                .encodeToString((m.usuario() + ":" + m.password()).getBytes(StandardCharsets.UTF_8));
        return builder
                .baseUrl(m.url())
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Basic " + credenciales)
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
}
