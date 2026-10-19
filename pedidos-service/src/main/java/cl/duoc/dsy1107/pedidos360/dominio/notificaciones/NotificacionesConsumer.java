package cl.duoc.dsy1107.pedidos360.dominio.notificaciones;

import cl.duoc.dsy1107.pedidos360.config.MensajeriaProperties;
import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ProcesadorMensajes;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor del dominio Notificaciones.
 * Escucha la cola configurada en pedidos360.rabbit.notificaciones.queue y delega
 * el negocio en {@link NotificacionService}. El ACK/NACK lo decide {@link ProcesadorMensajes}.
 */
@Component
public class NotificacionesConsumer {

    private final ProcesadorMensajes procesador;
    private final NotificacionService notificacionService;
    private final MensajeriaProperties props;

    public NotificacionesConsumer(ProcesadorMensajes procesador, NotificacionService notificacionService,
                                  MensajeriaProperties props) {
        this.procesador = procesador;
        this.notificacionService = notificacionService;
        this.props = props;
    }

    @RabbitListener(id = "consumidor-notificaciones", queues = "${pedidos360.rabbit.notificaciones.queue}")
    public void recibir(Message mensaje, Channel canal) throws IOException {
        procesador.procesar(props.notificaciones(), mensaje, canal, notificacionService::enviarConfirmacion);
    }
}
