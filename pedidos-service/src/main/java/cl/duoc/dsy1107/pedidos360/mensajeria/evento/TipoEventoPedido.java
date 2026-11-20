package cl.duoc.dsy1107.pedidos360.mensajeria.evento;

/**
 * Tipos de eventos del ciclo de vida de una orden en Pedidos360.
 */
public enum TipoEventoPedido {
    ORDER_CREATED("OrderCreated"),
    ORDER_ACCEPTED("OrderAccepted"),
    ORDER_PREPARING("OrderPreparing"),
    ORDER_DISPATCHED("OrderDispatched"),
    ORDER_DELIVERED("OrderDelivered"),
    ORDER_CANCELLED("OrderCancelled");

    private final String codigo;

    TipoEventoPedido(String codigo) {
        this.codigo = codigo;
    }

    public String getCodigo() {
        return codigo;
    }

    @Override
    public String toString() {
        return codigo;
    }
}
