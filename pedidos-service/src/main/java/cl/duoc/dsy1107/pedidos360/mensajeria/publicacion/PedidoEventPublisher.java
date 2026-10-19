package cl.duoc.dsy1107.pedidos360.mensajeria.publicacion;

import cl.duoc.dsy1107.pedidos360.mensajeria.evento.PedidoEvento;

/**
 * Puerto de salida para publicar eventos.
 * La capa de negocio depende de esta interfaz y no conoce exchanges ni routing keys.
 */
public interface PedidoEventPublisher {

    /** Publica el pedido aceptado hacia los dominios notificaciones, cocina y documentos. */
    void publicarPedidoAceptado(PedidoEvento evento);

    /**
     * Publica un evento de monitoreo en el topic exchange.
     *
     * @param routingKey clave del evento (ej. pedidos.pagos.confirmado); si es null se usa la configurada por defecto
     * @return la routing key efectivamente usada
     */
    String publicarEventoMonitoreo(String routingKey, Object payload);
}
