#!/bin/sh
# Pedidos360 - Script de Administración y Diagnóstico Replicable de Apache Kafka
#
# Uso: ./scripts/kafka-admin.sh <bloque>
# Bloques:
#   topics                - Describe todos los tópicos con particiones, líderes, réplicas e ISR
#   groups                - Lista los consumer groups registrados
#   lag                   - Describe el lag por partición de pedidos360-audit-group y pedidos360-metricas-group
#   retention             - Muestra y modifica la política retention.ms usando kafka-configs y AdminClient
#   crear-topico-prueba   - Crea el tópico 'topico.prueba' (3 particiones, RF=3)
#   borrar-topico-prueba  - Elimina el tópico 'topico.prueba'
#   fallo-broker          - Simula la caída de pedidos360-kafka3, evidencia ISR degradado y recuperación posterior

HOST="${HOST:-localhost}"
PEDIDOS="http://$HOST:8080"
KAFKA_CONTAINER="pedidos360-kafka1"
BOOTSTRAP="kafka-1:29092"
J="Content-Type: application/json"

titulo() { printf '\n==================== %s ====================\n' "$1"; }

case "$1" in
  topics)
    titulo "1. Tópicos y particiones (CLI Confluent cp-kafka)"
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe
    echo ""
    titulo "2. Tópicos y réplicas vía API REST (/api/kafka/admin/topics)"
    curl -s "$PEDIDOS/api/kafka/admin/topics"; echo
    ;;

  groups)
    titulo "1. Consumer Groups activos (CLI kafka-consumer-groups)"
    docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --list
    echo ""
    titulo "2. Detalle y miembros de Consumer Groups vía API REST (/api/kafka/admin/groups)"
    curl -s "$PEDIDOS/api/kafka/admin/groups"; echo
    ;;

  lag)
    titulo "1. Lag de 'pedidos360-audit-group' (CLI)"
    docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" \
      --describe --group pedidos360-audit-group
    echo ""
    titulo "2. Lag de 'pedidos360-metricas-group' (CLI)"
    docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" \
      --describe --group pedidos360-metricas-group
    echo ""
    titulo "3. Lag de 'pedidos360-timeline-group' (CLI)"
    docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" \
      --describe --group pedidos360-timeline-group
    echo ""
    titulo "4. Lag vía API REST (/api/kafka/admin/groups/pedidos360-metricas-group/lag)"
    curl -s "$PEDIDOS/api/kafka/admin/groups/pedidos360-metricas-group/lag"; echo
    ;;

  retention)
    TOPICO="${2:-orders.events}"
    NUEVA_RETENCION="${3:-86400000}" # 24 horas por defecto
    titulo "1. Configuración actual de retention.ms para '$TOPICO' (CLI)"
    docker exec "$KAFKA_CONTAINER" kafka-configs --bootstrap-server "$BOOTSTRAP" \
      --entity-type topics --entity-name "$TOPICO" --describe
    echo ""
    titulo "2. Modificando retention.ms a $NUEVA_RETENCION ms vía API REST (incrementalAlterConfigs)"
    curl -s -X PUT "$PEDIDOS/api/kafka/admin/topics/$TOPICO/retention?ms=$NUEVA_RETENCION"; echo
    echo ""
    titulo "3. Verificación de nueva configuración de retention.ms (CLI)"
    docker exec "$KAFKA_CONTAINER" kafka-configs --bootstrap-server "$BOOTSTRAP" \
      --entity-type topics --entity-name "$TOPICO" --describe
    ;;

  crear-topico-prueba)
    titulo "Creando tópico de prueba 'topico.prueba' (3 particiones, Factor Replicación = 3)"
    echo "--- Vía API REST (/api/kafka/admin/topics) ---"
    curl -s -X POST "$PEDIDOS/api/kafka/admin/topics" -H "$J" \
      -d '{"nombre":"topico.prueba","particiones":3,"replicas":3}'; echo
    echo ""
    echo "--- Verificación en CLI ---"
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic topico.prueba
    ;;

  borrar-topico-prueba)
    titulo "Eliminando tópico de prueba 'topico.prueba' vía API REST (/api/kafka/admin/topics/topico.prueba)"
    curl -s -X DELETE "$PEDIDOS/api/kafka/admin/topics/topico.prueba"; echo
    echo ""
    titulo "Intento de eliminar tópico protegido del sistema 'orders.events' (debe responder 400)"
    curl -s -X DELETE "$PEDIDOS/api/kafka/admin/topics/orders.events"; echo
    ;;

  fallo-broker)
    titulo "[Tolerancia a Fallos] 1. Estado inicial de particiones e ISR (3 brokers en sync)"
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic orders.events
    echo ""
    titulo "[Tolerancia a Fallos] 2. Deteniendo broker 3 ('pedidos360-kafka3')..."
    docker stop pedidos360-kafka3
    echo "Esperando 5 segundos a la detección del heartbeat..."
    sleep 5
    echo ""
    titulo "[Tolerancia a Fallos] 3. Estado con Broker caído -> ISR reducido a 2 réplicas"
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic orders.events
    echo ""
    titulo "[Tolerancia a Fallos] 4. Reiniciando broker 3 ('pedidos360-kafka3')..."
    docker start pedidos360-kafka3
    echo "Esperando 8 segundos a la sincronización y reincorporación al ISR..."
    sleep 8
    echo ""
    titulo "[Tolerancia a Fallos] 5. Estado final -> ISR restaurado completamente (1,2,3)"
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic orders.events
    ;;

  *)
    echo "Uso: ./scripts/kafka-admin.sh <bloque>"
    echo ""
    echo "Bloques disponibles:"
    echo "  topics               : Describe tópicos, particiones, réplicas e ISR"
    echo "  groups               : Lista todos los consumer groups y sus miembros"
    echo "  lag                  : Muestra committed offset, end offset y lag de los 2 grupos"
    echo "  retention [topico] [ms] : Consulta y altera retention.ms usando incrementalAlterConfigs"
    echo "  crear-topico-prueba  : Crea tópico 'topico.prueba' con 3 particiones y RF=3"
    echo "  borrar-topico-prueba : Elimina 'topico.prueba' y verifica bloqueo de tópicos base"
    echo "  fallo-broker         : Demuestra degradación y recuperación de ISR ante caída de broker"
    ;;
esac
