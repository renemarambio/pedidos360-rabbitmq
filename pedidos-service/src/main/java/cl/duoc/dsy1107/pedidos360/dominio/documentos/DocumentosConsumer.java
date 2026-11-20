package cl.duoc.dsy1107.pedidos360.dominio.documentos;

import cl.duoc.dsy1107.pedidos360.config.MensajeriaProperties;
import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ProcesadorMensajes;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor del dominio Documentos.
 * Escucha pedidos360.rabbit.documentos.queue y delega el negocio en {@link DocumentoService}.
 */
@Component
public class DocumentosConsumer {

    private final ProcesadorMensajes procesador;
    private final DocumentoService documentoService;
    private final MensajeriaProperties props;

    public DocumentosConsumer(ProcesadorMensajes procesador, DocumentoService documentoService,
                              MensajeriaProperties props) {
        this.procesador = procesador;
        this.documentoService = documentoService;
        this.props = props;
    }

    @RabbitListener(id = "consumidor-documentos", queues = "${pedidos360.rabbit.documentos.queue}")
    public void recibir(Message mensaje, Channel canal) throws IOException {
        procesador.procesar(props.documentos(), mensaje, canal, documentoService::generarBoleta);
    }
}
