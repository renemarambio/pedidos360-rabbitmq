package cl.duoc.dsy1107.pedidos360.dominio.cocina;

import cl.duoc.dsy1107.pedidos360.config.MensajeriaProperties;
import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ProcesadorMensajes;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor del dominio Cocina.
 * Escucha pedidos360.rabbit.cocina.queue y delega el negocio en {@link CocinaService}.
 */
@Component
public class CocinaConsumer {

    private final ProcesadorMensajes procesador;
    private final CocinaService cocinaService;
    private final MensajeriaProperties props;

    public CocinaConsumer(ProcesadorMensajes procesador, CocinaService cocinaService, MensajeriaProperties props) {
        this.procesador = procesador;
        this.cocinaService = cocinaService;
        this.props = props;
    }

    @RabbitListener(id = "consumidor-cocina", queues = "${pedidos360.rabbit.cocina.queue}")
    public void recibir(Message mensaje, Channel canal) throws IOException {
        procesador.procesar(props.cocina(), mensaje, canal, cocinaService::generarTicket);
    }
}
