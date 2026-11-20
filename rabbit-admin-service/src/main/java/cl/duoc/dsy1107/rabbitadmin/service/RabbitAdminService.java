package cl.duoc.dsy1107.rabbitadmin.service;

import cl.duoc.dsy1107.rabbitadmin.config.AdminProperties;
import cl.duoc.dsy1107.rabbitadmin.dto.BindingInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.BindingRequest;
import cl.duoc.dsy1107.rabbitadmin.dto.ColaInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.CrearColaRequest;
import cl.duoc.dsy1107.rabbitadmin.dto.CrearExchangeRequest;
import cl.duoc.dsy1107.rabbitadmin.dto.ExchangeInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.NodoInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.PoliticaInfo;
import cl.duoc.dsy1107.rabbitadmin.dto.PoliticaRequest;
import cl.duoc.dsy1107.rabbitadmin.exception.ConflictoException;
import cl.duoc.dsy1107.rabbitadmin.exception.OperacionNoPermitidaException;
import cl.duoc.dsy1107.rabbitadmin.exception.RecursoNoEncontradoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsula TODA la administración de RabbitMQ: colas, exchanges, bindings, políticas y nodos.
 * Los controladores solo llaman a estos métodos de alto nivel y no conocen la librería de RabbitMQ.
 *
 * - Crear/eliminar recursos: AMQP mediante {@link AmqpAdmin}.
 * - Consultar métricas, bindings, políticas y nodos: API HTTP de Management ({@link RabbitManagementClient}).
 */
@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

    private final AmqpAdmin amqpAdmin;
    private final RabbitManagementClient management;
    private final List<String> prefijosProtegidos;

    public RabbitAdminService(AmqpAdmin amqpAdmin, RabbitManagementClient management, AdminProperties props) {
        this.amqpAdmin = amqpAdmin;
        this.management = management;
        this.prefijosProtegidos = props.prefijosProtegidos();
    }

    // =====================================================================
    // COLAS
    // =====================================================================

    public ColaInfo crearCola(CrearColaRequest req) {
        validarNoProtegido(req.nombre(), "cola");
        if (amqpAdmin.getQueueInfo(req.nombre()) != null) {
            throw new ConflictoException("La cola '" + req.nombre() + "' ya existe");
        }
        if (tieneTexto(req.deadLetterExchange()) && management.obtenerExchange(req.deadLetterExchange()).isEmpty()) {
            throw new RecursoNoEncontradoException(
                    "El deadLetterExchange '" + req.deadLetterExchange() + "' no existe; créelo antes que la cola");
        }

        QueueBuilder builder = req.esDurable() ? QueueBuilder.durable(req.nombre()) : QueueBuilder.nonDurable(req.nombre());
        if (req.esAutoDelete()) {
            builder.autoDelete();
        }
        if (req.ttlMs() != null) {
            builder.ttl(req.ttlMs().intValue());
        }
        if (req.maxLength() != null) {
            builder.maxLength(req.maxLength().intValue());
        }
        if (tieneTexto(req.deadLetterExchange())) {
            builder.deadLetterExchange(req.deadLetterExchange());
            if (tieneTexto(req.deadLetterRoutingKey())) {
                builder.deadLetterRoutingKey(req.deadLetterRoutingKey());
            }
        }

        Queue cola = builder.build();
        amqpAdmin.declareQueue(cola);
        log.info("[ADMIN] Cola creada: {} (durable={}, args={})", cola.getName(), cola.isDurable(), cola.getArguments());

        return new ColaInfo(cola.getName(), 0, 0, 0, 0, cola.isDurable(), null, null, cola.getArguments());
    }

    public List<ColaInfo> listarColas() {
        return management.listarColas().stream().map(this::aColaInfo).toList();
    }

    public ColaInfo obtenerCola(String nombre) {
        return management.obtenerCola(nombre)
                .map(this::aColaInfo)
                .orElseThrow(() -> new RecursoNoEncontradoException("La cola '" + nombre + "' no existe"));
    }

    /**
     * Elimina una cola. Opcionalmente exige que esté vacía y/o sin consumidores
     * para no perder mensajes ni cortar un servicio que la está usando.
     */
    public void eliminarCola(String nombre, boolean soloSiVacia, boolean soloSiSinConsumidores) {
        validarNoProtegido(nombre, "cola");
        QueueInformation info = amqpAdmin.getQueueInfo(nombre);
        if (info == null) {
            throw new RecursoNoEncontradoException("La cola '" + nombre + "' no existe");
        }
        if (soloSiVacia && info.getMessageCount() > 0) {
            throw new ConflictoException("La cola '" + nombre + "' tiene " + info.getMessageCount() + " mensajes");
        }
        if (soloSiSinConsumidores && info.getConsumerCount() > 0) {
            throw new ConflictoException("La cola '" + nombre + "' tiene " + info.getConsumerCount() + " consumidores");
        }
        amqpAdmin.deleteQueue(nombre);
        log.info("[ADMIN] Cola eliminada: {}", nombre);
    }

    /** Limpieza: elimina los mensajes de la cola sin borrarla. Devuelve cuántos se eliminaron. */
    public int purgarCola(String nombre) {
        if (amqpAdmin.getQueueInfo(nombre) == null) {
            throw new RecursoNoEncontradoException("La cola '" + nombre + "' no existe");
        }
        int eliminados = amqpAdmin.purgeQueue(nombre);
        log.info("[ADMIN] Cola purgada: {} ({} mensajes eliminados)", nombre, eliminados);
        return eliminados;
    }

    // =====================================================================
    // EXCHANGES
    // =====================================================================

    public ExchangeInfo crearExchange(CrearExchangeRequest req) {
        validarNoProtegido(req.nombre(), "exchange");
        if (management.obtenerExchange(req.nombre()).isPresent()) {
            throw new ConflictoException("El exchange '" + req.nombre() + "' ya existe");
        }
        String tipo = req.tipo().name().toLowerCase();
        Exchange exchange = new ExchangeBuilder(req.nombre(), tipo).durable(req.esDurable()).build();
        amqpAdmin.declareExchange(exchange);
        log.info("[ADMIN] Exchange creado: {} (tipo={}, durable={})", req.nombre(), tipo, req.esDurable());
        return new ExchangeInfo(req.nombre(), tipo, req.esDurable());
    }

    public List<ExchangeInfo> listarExchanges() {
        return management.listarExchanges().stream()
                .filter(e -> tieneTexto(texto(e, "name")))          // omite el exchange por defecto ("")
                .map(e -> new ExchangeInfo(texto(e, "name"), texto(e, "type"), Boolean.TRUE.equals(e.get("durable"))))
                .toList();
    }

    public void eliminarExchange(String nombre) {
        validarNoProtegido(nombre, "exchange");
        if (management.obtenerExchange(nombre).isEmpty()) {
            throw new RecursoNoEncontradoException("El exchange '" + nombre + "' no existe");
        }
        amqpAdmin.deleteExchange(nombre);
        log.info("[ADMIN] Exchange eliminado: {}", nombre);
    }

    // =====================================================================
    // BINDINGS
    // =====================================================================

    public BindingInfo crearBinding(BindingRequest req) {
        if (management.obtenerExchange(req.exchange()).isEmpty()) {
            throw new RecursoNoEncontradoException("El exchange '" + req.exchange() + "' no existe");
        }
        if (amqpAdmin.getQueueInfo(req.cola()) == null) {
            throw new RecursoNoEncontradoException("La cola '" + req.cola() + "' no existe");
        }
        if (existeBinding(req)) {
            throw new ConflictoException("El binding " + req.exchange() + " -> " + req.cola()
                    + " con routing key '" + req.routingKey() + "' ya existe");
        }
        amqpAdmin.declareBinding(aBinding(req));
        log.info("[ADMIN] Binding creado: {} --[{}]--> {}", req.exchange(), req.routingKey(), req.cola());
        return new BindingInfo(req.exchange(), req.cola(), "queue", req.routingKey());
    }

    public List<BindingInfo> listarBindings() {
        return management.listarBindings().stream()
                .filter(b -> tieneTexto(texto(b, "source")))        // omite bindings automáticos del exchange por defecto
                .map(b -> new BindingInfo(texto(b, "source"), texto(b, "destination"),
                        texto(b, "destination_type"), texto(b, "routing_key")))
                .toList();
    }

    public void eliminarBinding(BindingRequest req) {
        if (!existeBinding(req)) {
            throw new RecursoNoEncontradoException("No existe el binding " + req.exchange() + " -> " + req.cola()
                    + " con routing key '" + req.routingKey() + "'");
        }
        amqpAdmin.removeBinding(aBinding(req));
        log.info("[ADMIN] Binding eliminado: {} --[{}]--> {}", req.exchange(), req.routingKey(), req.cola());
    }

    // =====================================================================
    // POLÍTICAS (retención y limpieza: TTL / max-length)
    // =====================================================================

    public PoliticaInfo aplicarPolitica(String nombre, PoliticaRequest req) {
        Map<String, Object> definicion = new LinkedHashMap<>();
        if (req.ttlMs() != null) {
            definicion.put("message-ttl", req.ttlMs());
        }
        if (req.maxLength() != null) {
            definicion.put("max-length", req.maxLength());
        }
        int prioridad = req.prioridad() != null ? req.prioridad() : 0;

        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("pattern", req.patron());
        cuerpo.put("definition", definicion);
        cuerpo.put("priority", prioridad);
        cuerpo.put("apply-to", "queues");

        management.guardarPolitica(nombre, cuerpo);
        log.info("[ADMIN] Política '{}' aplicada a colas '{}': {}", nombre, req.patron(), definicion);
        return new PoliticaInfo(nombre, req.patron(), "queues", prioridad, definicion);
    }

    public List<PoliticaInfo> listarPoliticas() {
        return management.listarPoliticas().stream().map(this::aPoliticaInfo).toList();
    }

    public void eliminarPolitica(String nombre) {
        if (management.obtenerPolitica(nombre).isEmpty()) {
            throw new RecursoNoEncontradoException("La política '" + nombre + "' no existe");
        }
        management.eliminarPolitica(nombre);
        log.info("[ADMIN] Política eliminada: {}", nombre);
    }

    // =====================================================================
    // CLÚSTER
    // =====================================================================

    public List<NodoInfo> listarNodos() {
        return management.listarNodos().stream()
                .map(n -> new NodoInfo(texto(n, "name"), Boolean.TRUE.equals(n.get("running")), texto(n, "type")))
                .toList();
    }

    // =====================================================================
    // Apoyo
    // =====================================================================

    private void validarNoProtegido(String nombre, String recurso) {
        prefijosProtegidos.stream()
                .filter(nombre::startsWith)
                .findFirst()
                .ifPresent(prefijo -> {
                    throw new OperacionNoPermitidaException(
                            "El prefijo '" + prefijo + "' está reservado; no se puede administrar la " + recurso
                                    + " '" + nombre + "'");
                });
    }

    private boolean existeBinding(BindingRequest req) {
        return management.bindingsEntre(req.exchange(), req.cola()).stream()
                .anyMatch(b -> Objects.equals(req.routingKey(), texto(b, "routing_key")));
    }

    private Binding aBinding(BindingRequest req) {
        return new Binding(req.cola(), Binding.DestinationType.QUEUE, req.exchange(), req.routingKey(), null);
    }

    @SuppressWarnings("unchecked")
    private ColaInfo aColaInfo(Map<String, Object> c) {
        Object argumentos = c.get("arguments");
        return new ColaInfo(
                texto(c, "name"),
                numero(c, "messages_ready"),
                numero(c, "messages_unacknowledged"),
                numero(c, "messages"),
                (int) numero(c, "consumers"),
                Boolean.TRUE.equals(c.get("durable")),
                texto(c, "node"),
                texto(c, "policy"),
                argumentos instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of());
    }

    @SuppressWarnings("unchecked")
    private PoliticaInfo aPoliticaInfo(Map<String, Object> p) {
        Object definicion = p.get("definition");
        return new PoliticaInfo(
                texto(p, "name"),
                texto(p, "pattern"),
                texto(p, "apply-to"),
                (int) numero(p, "priority"),
                definicion instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of());
    }

    private static long numero(Map<String, Object> mapa, String clave) {
        return mapa.get(clave) instanceof Number n ? n.longValue() : 0L;
    }

    private static String texto(Map<String, Object> mapa, String clave) {
        Object valor = mapa.get(clave);
        return valor != null ? valor.toString() : null;
    }

    private static boolean tieneTexto(String valor) {
        return valor != null && !valor.isBlank();
    }
}
