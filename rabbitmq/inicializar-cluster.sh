#!/bin/sh
# Verifica que rabbit2 esté unido a rabbit1 (lo une si hace falta) y aplica
# la política de retención de la cola de monitoreo. Se ejecuta una sola vez
# desde el servicio "rabbit-init" del docker-compose.
set -e

CTL="rabbitmqctl -q"

echo "[init] Esperando a que ambos nodos estén operativos..."
$CTL -n rabbit@rabbit1 await_startup
$CTL -n rabbit@rabbit2 await_startup

if $CTL -n rabbit@rabbit1 cluster_status | grep -q "rabbit@rabbit2"; then
  echo "[init] rabbit2 ya pertenece al clúster de rabbit1."
else
  echo "[init] Uniendo rabbit2 al clúster de rabbit1..."
  $CTL -n rabbit@rabbit2 stop_app
  $CTL -n rabbit@rabbit2 reset
  $CTL -n rabbit@rabbit2 join_cluster rabbit@rabbit1
  $CTL -n rabbit@rabbit2 start_app
fi

echo "[init] Política de retención/limpieza para la cola de monitoreo (TTL 1 h, máximo 500 mensajes)..."
$CTL -n rabbit@rabbit1 set_policy retencion-monitoreo '^pedidos360\.monitoreo\.' \
  '{"message-ttl":3600000,"max-length":500}' --priority 1 --apply-to queues

echo "[init] Estado del clúster:"
rabbitmqctl -n rabbit@rabbit1 cluster_status
echo "[init] Listo."
