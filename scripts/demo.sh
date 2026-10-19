#!/bin/sh
# Comandos de demostración para el video EP4.
# Uso:  ./scripts/demo.sh <bloque>      (ej. ./scripts/demo.sh ep4-07)
# Variables opcionales: HOST (por defecto localhost)

HOST="${HOST:-localhost}"
PEDIDOS="http://$HOST:8080"
ADMIN="http://$HOST:8081/api/admin"
J="Content-Type: application/json"
# Credenciales de RabbitMQ (se leen de .env si existe; no se muestran en pantalla)
[ -f .env ] && . ./.env
RABBIT_USER="${RABBITMQ_USER:-labuser}"
RABBIT_PASS="${RABBITMQ_PASSWORD:-labpass}"

titulo() { printf '\n========== %s ==========\n' "$1"; }

case "$1" in
  ep4-01|cluster)
    titulo "[EP4-01/02] Nodos y estado del clúster"
    docker exec pedidos360-rabbit1 rabbitmqctl cluster_status
    curl -s "$ADMIN/cluster/nodos"; echo
    ;;

  ep4-03|compose)
    titulo "[EP4-03] Contenedores levantados con Docker Compose"
    docker compose ps
    ;;

  ep4-04|colas)
    titulo "[EP4-04/05/06] Colas, DLQ y exchanges"
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
    titulo "Evento al topic exchange (lo recibe la cola de monitoreo con pedidos.#)"
    curl -s -X POST "$PEDIDOS/api/lab/eventos/topic?routingKey=pedidos.pagos.confirmado" -H "$J" \
      -d '{"evento":"PEDIDO_PAGADO","pedidoId":2001}'; echo
    ;;

  *)
    echo "Bloques: ep4-01 ep4-03 ep4-04 ep4-07 ep4-08 ep4-08b ep4-09 ep4-09b ep4-10 ep4-11 topic"
    ;;
esac
