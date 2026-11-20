package cl.duoc.dsy1107.pedidos360.mensajeria;

import cl.duoc.dsy1107.pedidos360.config.MensajeriaProperties;
import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ErrorProcesamientoException;
import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ProcesadorMensajes;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Pruebas de la política de ACK / reintento / DLQ sin necesidad de RabbitMQ.
 */
class ProcesadorMensajesTest {

    private static final long TAG = 7L;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private RabbitTemplate rabbitTemplate;
    private Channel canal;
    private ProcesadorMensajes procesador;
    private MensajeriaProperties.Dominio cocina;

    @BeforeEach
    void preparar() {
        rabbitTemplate = mock(RabbitTemplate.class);
        canal = mock(Channel.class);
        cocina = new MensajeriaProperties.Dominio("cocina", "pedidos360.cocina.queue", "pedidos360.cocina.retry",
                "pedidos360.cocina.dlq", "cocina", "cocina.retry", "cocina.failed");
        MensajeriaProperties props = new MensajeriaProperties(
                new MensajeriaProperties.Exchanges("pedidos360.direct.exchange", "pedidos360.topic.exchange", "pedidos360.dlx"),
                new MensajeriaProperties.Reintentos(2, 5000, "x-lab-intentos"),
                new MensajeriaProperties.RetencionDlq(86400000, 1000),
                cocina, cocina, cocina,
                new MensajeriaProperties.Monitoreo("pedidos360.monitoreo.queue", "pedidos.#", "pedidos.evento.nuevo"),
                new MensajeriaProperties.AuditoriaDlq(true));
        procesador = new ProcesadorMensajes(rabbitTemplate, objectMapper, props);
    }

    @Test
    void procesadoConExito_haceAck() throws Exception {
        procesador.procesar(cocina, mensaje(evento(), 0), canal, e -> { });

        verify(canal).basicAck(TAG, false);
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void errorRecuperableConIntentosDisponibles_publicaReintentoYConfirmaOriginal() throws Exception {
        procesador.procesar(cocina, mensaje(evento(), 0), canal, e -> {
            throw ErrorProcesamientoException.recuperable("impresora sin conexión");
        });

        verify(rabbitTemplate).send(eq("pedidos360.direct.exchange"), eq("cocina.retry"),
                argThat(m -> Integer.valueOf(1).equals(m.getMessageProperties().getHeader("x-lab-intentos"))));
        verify(canal).basicAck(TAG, false);
    }

    @Test
    void errorRecuperableSinIntentos_haceNackHaciaDlq() throws Exception {
        procesador.procesar(cocina, mensaje(evento(), 2), canal, e -> {
            throw ErrorProcesamientoException.recuperable("impresora sin conexión");
        });

        verify(canal).basicNack(TAG, false, false);
        verify(rabbitTemplate, never()).send(anyString(), anyString(), any(Message.class));
    }

    @Test
    void errorNoRecuperable_vaDirectoADlqSinReintentar() throws Exception {
        procesador.procesar(cocina, mensaje(evento(), 0), canal, e -> {
            throw ErrorProcesamientoException.noRecuperable("datos inválidos");
        });

        verify(canal).basicNack(TAG, false, false);
        verify(rabbitTemplate, never()).send(anyString(), anyString(), any(Message.class));
    }

    @Test
    void mensajeIlegible_vaDirectoADlq() throws Exception {
        Message basura = new Message("esto no es json".getBytes(StandardCharsets.UTF_8), propiedades(0));

        procesador.procesar(cocina, basura, canal, e -> { });

        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
    }

    private PedidoEvento evento() {
        return new PedidoEvento("evt-1", 1001L, "cliente@duoc.cl", BigDecimal.valueOf(25000),
                false, false, false, Map.of("origen", "TEST"));
    }

    private Message mensaje(PedidoEvento evento, int intentos) throws Exception {
        return new Message(objectMapper.writeValueAsBytes(evento), propiedades(intentos));
    }

    private MessageProperties propiedades(int intentos) {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setHeader("x-lab-intentos", intentos);
        return props;
    }
}
