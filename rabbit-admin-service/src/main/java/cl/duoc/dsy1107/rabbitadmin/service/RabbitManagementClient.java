package cl.duoc.dsy1107.rabbitadmin.service;

import cl.duoc.dsy1107.rabbitadmin.config.AdminProperties;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Acceso a la API HTTP de RabbitMQ Management.
 * Se usa para lo que AMQP no ofrece: listar recursos, leer métricas, bindings, políticas y nodos.
 * Solo lo utiliza {@link RabbitAdminService}.
 */
@Component
public class RabbitManagementClient {

    private static final ParameterizedTypeReference<List<Map<String, Object>>> LISTA =
            new ParameterizedTypeReference<>() { };
    private static final ParameterizedTypeReference<Map<String, Object>> OBJETO =
            new ParameterizedTypeReference<>() { };

    private final RestClient http;
    private final String vhost;

    public RabbitManagementClient(RestClient rabbitManagementRestClient, AdminProperties props) {
        this.http = rabbitManagementRestClient;
        this.vhost = props.management().vhost();
    }

    public List<Map<String, Object>> listarColas() {
        return lista("/api/queues/{vhost}", vhost);
    }

    public Optional<Map<String, Object>> obtenerCola(String nombre) {
        return objeto("/api/queues/{vhost}/{nombre}", vhost, nombre);
    }

    public List<Map<String, Object>> listarExchanges() {
        return lista("/api/exchanges/{vhost}", vhost);
    }

    public Optional<Map<String, Object>> obtenerExchange(String nombre) {
        return objeto("/api/exchanges/{vhost}/{nombre}", vhost, nombre);
    }

    public List<Map<String, Object>> listarBindings() {
        return lista("/api/bindings/{vhost}", vhost);
    }

    public List<Map<String, Object>> bindingsEntre(String exchange, String cola) {
        return lista("/api/bindings/{vhost}/e/{exchange}/q/{cola}", vhost, exchange, cola);
    }

    public List<Map<String, Object>> listarPoliticas() {
        return lista("/api/policies/{vhost}", vhost);
    }

    public Optional<Map<String, Object>> obtenerPolitica(String nombre) {
        return objeto("/api/policies/{vhost}/{nombre}", vhost, nombre);
    }

    public void guardarPolitica(String nombre, Map<String, Object> cuerpo) {
        http.put().uri("/api/policies/{vhost}/{nombre}", vhost, nombre)
                .contentType(MediaType.APPLICATION_JSON)
                .body(cuerpo)
                .retrieve()
                .toBodilessEntity();
    }

    public void eliminarPolitica(String nombre) {
        http.delete().uri("/api/policies/{vhost}/{nombre}", vhost, nombre)
                .retrieve()
                .toBodilessEntity();
    }

    public List<Map<String, Object>> listarNodos() {
        return lista("/api/nodes");
    }

    private List<Map<String, Object>> lista(String uri, Object... variables) {
        List<Map<String, Object>> resultado = http.get().uri(uri, variables).retrieve().body(LISTA);
        return resultado != null ? resultado : List.of();
    }

    private Optional<Map<String, Object>> objeto(String uri, Object... variables) {
        try {
            return Optional.ofNullable(http.get().uri(uri, variables).retrieve().body(OBJETO));
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        }
    }
}
