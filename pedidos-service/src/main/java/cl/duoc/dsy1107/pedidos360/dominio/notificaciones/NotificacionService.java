package cl.duoc.dsy1107.pedidos360.dominio.notificaciones;

import cl.duoc.dsy1107.pedidos360.mensajeria.consumo.ErrorProcesamientoException;
import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Lógica de negocio del dominio Notificaciones: confirma el pedido al cliente por correo.
 * No conoce RabbitMQ; solo recibe el evento y lanza un error si no puede cumplir su tarea.
 */
@Service
public class NotificacionService {

    private static final Logger log = LoggerFactory.getLogger(NotificacionService.class);

    public void enviarConfirmacion(PedidoEvento evento) {
        if (evento.clienteEmail() == null || !evento.clienteEmail().contains("@")) {
            throw ErrorProcesamientoException.noRecuperable(
                    "correo del cliente inválido: " + evento.clienteEmail());
        }
        if (evento.falloNotificaciones()) {
            throw ErrorProcesamientoException.recuperable("servidor de correo no disponible (fallo simulado)");
        }
        log.info("[NOTIFICACIONES] Correo de confirmación enviado a {} por el pedido {} (total ${})",
                evento.clienteEmail(), evento.pedidoId(), evento.total());
    }
}
