#!/bin/sh
# Comandos de demostración para el laboratorio y evaluaciones de Pedidos360.
# Soporta tanto mensajería asíncrona con RabbitMQ (EP3/EP4) como streaming de eventos con Apache Kafka (EP5/EP6).
# Uso:  ./scripts/demo.sh <bloque>      (ej. ./scripts/demo.sh ep5-ciclo)
# Variables opcionales: HOST (por defecto localhost)

HOST="${HOST:-localhost}"
PEDIDOS="http://$HOST:8080"
ADMIN="http://$HOST:8081/api/admin"
KAFKA_UI="http://$HOST:8090"
J="Content-Type: application/json"
# Credenciales de RabbitMQ (se leen de .env si existe; no se muestran en pantalla)
[ -f .env ] && . ./.env
RABBIT_USER="${RABBITMQ_USER:-labuser}"
RABBIT_PASS="${RABBITMQ_PASSWORD:-labpass}"

titulo() { printf '\n========== %s ==========\n' "$1"; }

case "$1" in
  # -------------------------------------------------------------
  # BLOQUES RABBITMQ (EP3 / EP4)
  # -------------------------------------------------------------
  ep4-01|cluster)
    titulo "[EP4-01/02] Nodos y estado del clúster RabbitMQ"
    docker exec pedidos360-rabbit1 rabbitmqctl cluster_status
    curl -s "$ADMIN/cluster/nodos"; echo
    ;;

  ep4-03|compose)
    titulo "[EP4-03] Contenedores levantados con Docker Compose"
    docker compose ps
    ;;

  ep4-04|colas)
    titulo "[EP4-04/05/06] Colas, DLQ y exchanges RabbitMQ"
    docker exec pedidos360-rabbit1 rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
    docker exec pedidos360-rabbit1 rabbitmqctl list_exchanges name type | grep -E "pedidos360|name"
    ;;

  ep4-07|flujo)
    titulo "[EP4-07] Productor -> exchange -> cola -> consumidor (pedido exitoso)"
    curl -s -X POST "$PEDIDOS/api/lab/pedidos/aceptar" -H "$J" \
      -d '{"pedidoId":2001,"clienteEmail":"cliente@duoc.cl","total":25990}'; echo
    echo "Revise los logs:  docker compose logs --tail=30 pedidos-service"
    ;;

  ep4-08|fallo)
    titulo "[EP4-08] Fallo controlado en cocina -> 2 reintentos (TTL 5 s) -> DLQ -> log"
    curl -s -X POST "$PEDIDOS/api/lab/pedidos/aceptar" -H "$J" \
      -d '{"pedidoId":2002,"clienteEmail":"cliente@duoc.cl","total":18500,"falloCocina":true}'; echo
    echo "Esperando 15 s a que se agoten los reintentos..."
    sleep 15
    docker compose logs --tail=40 pedidos-service | grep -E "COCINA|DLQ"
    ;;

  ep4-08b|no-recuperable)
    titulo "[EP4-08] Mensaje ilegible publicado directo en RabbitMQ -> NACK sin reintentos -> DLQ de documentos"
    curl -s -u "$RABBIT_USER:$RABBIT_PASS" -X POST \
      "http://$HOST:15672/api/exchanges/%2F/pedidos360.direct.exchange/publish" -H "$J" \
      -d '{"properties":{},"routing_key":"documentos","payload":"esto no es un JSON valido","payload_encoding":"string"}'; echo
    sleep 2
    docker compose logs --tail=20 pedidos-service | grep -E "DOCUMENTOS|DLQ"
    ;;

  ep4-09|admin)
    titulo "[EP4-09] Microservicio administrador: crear exchange, cola y binding"
    curl -s -X POST "$ADMIN/exchanges" -H "$J" -d '{"nombre":"demo.pagos.topic","tipo":"TOPIC"}'; echo
    curl -s -X POST "$ADMIN/queues" -H "$J" -d '{"nombre":"demo.pagos.queue","ttlMs":60000,"maxLength":100}'; echo
    curl -s -X POST "$ADMIN/bindings" -H "$J" \
      -d '{"exchange":"demo.pagos.topic","cola":"demo.pagos.queue","routingKey":"pagos.*.confirmado"}'; echo
    titulo "Validación: nombre vacío y TTL negativo (debe responder 400)"
    curl -s -X POST "$ADMIN/queues" -H "$J" -d '{"nombre":"","ttlMs":-1}'; echo
    ;;

  ep4-09b|admin-eliminar)
    titulo "[EP4-09] Eliminar binding, cola y exchange de prueba"
    curl -s -o /dev/null -w "DELETE binding  -> %{http_code}\n" -X DELETE "$ADMIN/bindings" -H "$J" \
      -d '{"exchange":"demo.pagos.topic","cola":"demo.pagos.queue","routingKey":"pagos.*.confirmado"}'
    curl -s -o /dev/null -w "DELETE cola     -> %{http_code}\n" -X DELETE "$ADMIN/queues/demo.pagos.queue"
    curl -s -o /dev/null -w "DELETE exchange -> %{http_code}\n" -X DELETE "$ADMIN/exchanges/demo.pagos.topic"
    ;;

  ep4-10|metricas)
    titulo "[EP4-10] Ready / Unacked / Consumers / mensajes en DLQ"
    curl -s "$ADMIN/queues"; echo
    ;;

  ep4-11|politicas)
    titulo "[EP4-11] Políticas de retención y limpieza"
    curl -s -X PUT "$ADMIN/policies/retencion-demo" -H "$J" \
      -d '{"patron":"^demo\\.","ttlMs":60000,"maxLength":100}'; echo
    curl -s "$ADMIN/policies"; echo
    titulo "Limpieza manual de una cola (purge)"
    curl -s -X DELETE "$ADMIN/queues/pedidos360.monitoreo.queue/mensajes"; echo
    ;;

  topic)
    titulo "Evento al topic exchange RabbitMQ (lo recibe la cola de monitoreo con pedidos.#)"
    curl -s -X POST "$PEDIDOS/api/lab/eventos/topic?routingKey=pedidos.pagos.confirmado" -H "$J" \
      -d '{"evento":"PEDIDO_PAGADO","pedidoId":2001}'; echo
    ;;

  # -------------------------------------------------------------
  # BLOQUES APACHE KAFKA (EP5 / EP6)
  # -------------------------------------------------------------
  ep5-01|ep5-status|kafka-status)
    titulo "[EP5-01] Estado del microservicio con integración Kafka + RabbitMQ"
    curl -s "$PEDIDOS/api/status"; echo
    titulo "Listar tópicos creados en Apache Kafka (3 Brokers, RF=3)"
    docker exec pedidos360-kafka1 kafka-topics --bootstrap-server kafka-1:29092 --list
    ;;

  ep5-02|ep5-crear|kafka-crear)
    titulo "[EP5-02] Crear Pedido desde Catálogo -> publica en RabbitMQ y streaming OrderCreated a Kafka"
    curl -s -X POST "$PEDIDOS/api/pedidos" -H "$J" \
      -d '{"idPedido":"PED-3001","productoId":"PROD-BURGER","nombreProducto":"Hamburguesa Doble Queso 360","precio":7990,"cantidad":2}'; echo
    echo ""
    echo "Verifique logs del consumidor de auditoría Kafka:"
    docker compose logs --tail=25 pedidos-service | grep -E "AUDITORÍA KAFKA|KAFKA-PRODUCTOR"
    ;;

  ep5-03|ep5-ciclo|kafka-ciclo)
    titulo "[EP5-03] Ciclo de vida completo de un pedido en Apache Kafka (mismo Key=pedidoId -> orden estricto)"
    PEDIDO_ID=3050
    echo "1. Creando pedido (OrderCreated)..."
    curl -s -X POST "$PEDIDOS/api/pedidos" -H "$J" \
      -d '{"idPedido":"PED-'"$PEDIDO_ID"'","productoId":"PROD-PIZZA","nombreProducto":"Pizza Familiar 360","precio":12990,"cantidad":1}'; echo
    sleep 1

    echo "2. Aceptando pedido (OrderAccepted)..."
    curl -s -X POST "$PEDIDOS/api/lab/pedidos/aceptar" -H "$J" \
      -d '{"pedidoId":'"$PEDIDO_ID"',"clienteEmail":"cliente'"$PEDIDO_ID"'@duoc.cl","total":12990}'; echo
    sleep 1

    echo "3. Cocina preparando ticket (OrderPreparing)..."
    curl -s -X POST "$PEDIDOS/api/pedidos/$PEDIDO_ID/estado" -H "$J" \
      -d '{"estado":"OrderPreparing","responsable":"Chef Juan","estacion":"Horno 2"}'; echo
    sleep 1

    echo "4. Despachando pedido (OrderDispatched)..."
    curl -s -X POST "$PEDIDOS/api/pedidos/$PEDIDO_ID/estado" -H "$J" \
      -d '{"estado":"OrderDispatched","repartidor":"Carlos Delivery","vehiculo":"Moto-04"}'; echo
    sleep 1

    echo "5. Entregando pedido al cliente (OrderDelivered)..."
    curl -s -X POST "$PEDIDOS/api/pedidos/$PEDIDO_ID/entregar" -H "$J" \
      -d '{"calificacion":5,"comentarios":"Llego caliente y a tiempo"}'; echo

    titulo "Logs de auditoría para pedido $PEDIDO_ID (demostrando clave pedidoId y orden cronológico)"
    docker compose logs --tail=50 pedidos-service | grep -E "AUDITORÍA KAFKA|Tipo de Evento|Clave \(Key\)"
    ;;

  ep5-04|ep5-dlt|kafka-dlt)
    titulo "[EP5-04] Manejo de fallos en Kafka: reintentos y desvío hacia Dead Letter Topic ('orders.events.DLT')"
    curl -s -X POST "$PEDIDOS/api/lab/kafka/simular-dlt?pedidoId=9999" -H "$J"; echo
    echo ""
    echo "Esperando 4 segundos a que se agoten los reintentos (2 reintentos con intervalo de 1s)..."
    sleep 4
    titulo "Logs de reintentos y llegada a DLT en pedidos-service:"
    docker compose logs --tail=40 pedidos-service | grep -E "KAFKA-REINTENTO|KAFKA-DLT|DEAD LETTER TOPIC"
    ;;

  ep5-05|ep5-ui|kafka-ui)
    titulo "[EP5-05] Kafka UI - Panel Web de Administración"
    echo "Panel Kafka UI disponible en:"
    echo "  $KAFKA_UI"
    echo ""
    echo "Tópicos listados en la UI:"
    echo "  - orders.events    (3 particiones, RF=3)"
    echo "  - audit.timeline   (3 particiones, RF=3)"
    echo "  - orders.events.DLT (Dead Letter Topic, RF=3)"
    echo "Consumer Groups activos en Kafka UI:"
    echo "  - pedidos360-audit-group     (orders.events)"
    echo "  - pedidos360-metricas-group  (orders.events, concurrency=3)"
    echo "  - pedidos360-timeline-group  (audit.timeline)"
    ;;

  timeline|ep5-timeline)
    titulo "[EP5-06] Línea de tiempo de auditoría por pedido (GET /api/pedidos/{id}/timeline)"
    PEDIDO_ID="${2:-3050}"
    echo "Consultando timeline para pedidoId=$PEDIDO_ID:"
    curl -s "$PEDIDOS/api/pedidos/$PEDIDO_ID/timeline"; echo
    ;;

  metricas|ep5-metricas)
    titulo "[EP5-07] Métricas en tiempo real segundo consumer group (GET /api/kafka/metricas)"
    curl -s "$PEDIDOS/api/kafka/metricas"; echo
    ;;

  grupos|ep5-grupos)
    titulo "[EP5-08] Consumer Groups activos (AdminClient API + Kafka CLI)"
    echo "--- Desde API REST (GET /api/kafka/admin/groups) ---"
    curl -s "$PEDIDOS/api/kafka/admin/groups"; echo
    echo ""
    echo "--- Desde CLI (kafka-consumer-groups) ---"
    docker exec pedidos360-kafka1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --list
    ;;

  lag|ep5-lag)
    titulo "[EP5-09] Lag por partición de los Consumer Groups"
    echo "--- Lag pedidos360-metricas-group (API REST) ---"
    curl -s "$PEDIDOS/api/kafka/admin/groups/pedidos360-metricas-group/lag"; echo
    echo ""
    echo "--- Lag pedidos360-audit-group (API REST) ---"
    curl -s "$PEDIDOS/api/kafka/admin/groups/pedidos360-audit-group/lag"; echo
    echo ""
    echo "--- Lag pedidos360-metricas-group (CLI) ---"
    docker exec pedidos360-kafka1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group pedidos360-metricas-group
    echo ""
    echo "--- Lag pedidos360-audit-group (CLI) ---"
    docker exec pedidos360-kafka1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group pedidos360-audit-group
    ;;

  *)
    echo "Uso: ./scripts/demo.sh <bloque>"
    echo ""
    echo "Bloques RabbitMQ (EP3 / EP4):"
    echo "  ep4-01 | cluster       : Nodos y estado del clúster RabbitMQ"
    echo "  ep4-03 | compose       : Contenedores docker compose"
    echo "  ep4-04 | colas         : Colas, DLQ y exchanges"
    echo "  ep4-07 | flujo         : Pedido exitoso en RabbitMQ"
    echo "  ep4-08 | fallo         : Fallo controlado -> reintentos -> DLQ"
    echo "  ep4-08b| no-recuperable: Mensaje no serializable directo a DLQ"
    echo "  ep4-09 | admin         : API de administración RabbitMQ"
    echo "  ep4-10 | metricas      : Métricas Ready / Unacked / DLQ"
    echo "  ep4-11 | politicas     : Políticas de retención y limpieza"
    echo "  topic                  : Evento topic RabbitMQ"
    echo ""
    echo "Bloques Apache Kafka (EP5 / EP6):"
    echo "  ep5-status | ep5-01    : Estado de Kafka, tópicos y microservicio"
    echo "  ep5-crear  | ep5-02    : Creación de pedido y streaming OrderCreated"
    echo "  ep5-ciclo  | ep5-03    : Ciclo de vida completo (OrderCreated -> Accepted -> Preparing -> Dispatched -> Delivered)"
    echo "  ep5-dlt    | ep5-04    : Demostración de reintentos y Dead Letter Topic (orders.events.DLT)"
    echo "  ep5-ui     | ep5-05    : Acceso al panel web Kafka UI en puerto 8090"
    echo "  timeline               : Consulta de auditoría en memoria (GET /api/pedidos/{id}/timeline)"
    echo "  metricas               : Contadores del 2do grupo independiente (GET /api/kafka/metricas)"
    echo "  grupos                 : Lista de consumer groups activos y miembros"
    echo "  lag                    : Committed offset, end offset y lag por partición"
    ;;
esac
