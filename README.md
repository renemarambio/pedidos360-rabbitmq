# Pedidos360 — Mensajería asíncrona con RabbitMQ

**DSY1107 Desarrollo Cloud Native I** · Evaluaciones Parciales 3 (Encargo) y 4 (Presentación)

Integrantes: Rene Marambio · _(completar)_

Pedidos360 recibe pedidos por REST y los distribuye de forma asíncrona a tres dominios: **notificaciones** (correo al cliente), **cocina** (ticket de preparación) y **documentos** (boleta). Cada dominio tiene su cola, una cola de reintento con TTL y una DLQ. Un segundo microservicio administra colas, exchanges, bindings y políticas mediante una API REST.

## Arquitectura

```mermaid
flowchart LR
    W[Catálogo web / curl] -->|POST /api/pedidos| P[pedidos-service :8080]
    P -->|productor| DX{{pedidos360.direct.exchange<br/>direct}}
    DX -->|notificaciones| QN[notificaciones.queue]
    DX -->|cocina| QC[cocina.queue]
    DX -->|documentos| QD[documentos.queue]
    QN & QC & QD -->|@RabbitListener + ACK| P
    P -. error recuperable .->|*.retry| R[colas retry<br/>TTL 5 s]
    R -. vuelve a la cola .-> DX
    QN & QC & QD -. NACK .-> DLX{{pedidos360.dlx}}
    DLX --> DLQ[3 DLQ] -->|log del fallo| P
    P -->|pedidos.*| TX{{pedidos360.topic.exchange<br/>topic}} -->|pedidos.#| QM[monitoreo.queue]
    A[rabbit-admin-service :8081] -->|AMQP + API Management| RB[(Clúster RabbitMQ<br/>rabbit1 + rabbit2)]
```

| Componente | Descripción |
|---|---|
| `pedidos-service` | Productor y consumidores. Spring Boot 3.3, Java 21. |
| `rabbit-admin-service` | API REST para administrar RabbitMQ. Documentación Swagger en `/swagger-ui.html`. |
| `rabbit1`, `rabbit2` | Clúster RabbitMQ 3.13 de 2 nodos (`pedidos360-cluster`). |
| `rabbit-init` | Tarea única que verifica el clúster y aplica la política de retención. |

## Topología

Todos los nombres están en `pedidos-service/src/main/resources/application.yml` (prefijo `pedidos360.rabbit`). Se leen con `MensajeriaProperties` y no hay nombres escritos en el código de negocio.

| Exchange | Tipo | Binding (routing key) | Cola |
|---|---|---|---|
| `pedidos360.direct.exchange` | direct | `notificaciones` / `cocina` / `documentos` | `pedidos360.<dominio>.queue` |
| `pedidos360.direct.exchange` | direct | `<dominio>.retry` | `pedidos360.<dominio>.retry` (TTL 5 s, vuelve a la cola principal) |
| `pedidos360.dlx` | direct | `<dominio>.failed` | `pedidos360.<dominio>.dlq` (retención 24 h, máx. 1000 mensajes) |
| `pedidos360.topic.exchange` | topic | `pedidos.#` | `pedidos360.monitoreo.queue` (política: TTL 1 h, máx. 500) |

## Manejo de ACK y errores

Lo resuelve `ProcesadorMensajes`, usado por los tres consumidores. Siempre con ACK manual y nunca con `requeue=true`, para evitar ciclos infinitos.

| Situación | Acción |
|---|---|
| Procesado correctamente | `basicAck` |
| Mensaje ilegible o sin `pedidoId` | `basicNack(requeue=false)` → DLQ, sin reintentos |
| Error **no recuperable** del dominio | `basicNack(requeue=false)` → DLQ |
| Error **recuperable** con intentos disponibles | copia a la cola `*.retry` (header `x-lab-intentos` + 1) y `basicAck` del original |
| Error recuperable con 2 intentos agotados | `basicNack(requeue=false)` → DLQ |
| No se pudo publicar el reintento | `basicNack(requeue=false)` → DLQ (no se pierde el mensaje) |

`DeadLetterAuditConsumer` lee las tres DLQ y deja en el log la cola de origen, el motivo y la cantidad de rechazos (header `x-death`). Con `AUDITORIA_DLQ_HABILITADA=false` los mensajes quedan acumulados en las DLQ, para mostrarlos en la Management UI.

## Estructura del código

```
pedidos-service/src/main/java/cl/duoc/dsy1107/pedidos360/
├── config/                 MensajeriaProperties (nombres), RabbitTopologiaConfig (beans), RabbitInfraestructuraConfig
├── mensajeria/
│   ├── evento/             PedidoEvento
│   ├── publicacion/        PedidoEventPublisher (interfaz) + RabbitPedidoEventPublisher (productor)
│   └── consumo/            ProcesadorMensajes (ACK/NACK/reintento/DLQ), ErrorProcesamientoException
├── dominio/
│   ├── notificaciones/     NotificacionesConsumer + NotificacionService
│   ├── cocina/             CocinaConsumer + CocinaService
│   ├── documentos/         DocumentosConsumer + DocumentoService
│   ├── monitoreo/          MonitoreoConsumer (topic)
│   └── auditoria/          DeadLetterAuditConsumer (log de las DLQ)
└── pedidos/                PedidoController, DTO validados, PedidoService

rabbit-admin-service/src/main/java/cl/duoc/dsy1107/rabbitadmin/
├── api/                    QueueController, ExchangeController, BindingController, PoliticaController, ClusterController
├── dto/                    Requests con validación (@NotBlank, @Pattern, @Positive, @AssertTrue) y respuestas
├── service/                RabbitAdminService (toda la lógica) + RabbitManagementClient
└── exception/              Errores 400 / 403 / 404 / 409 / 502 / 503 en JSON
```

## API del administrador (`http://<host>:8081`)

| Método | Ruta | Uso | Respuestas |
|---|---|---|---|
| POST | `/api/admin/queues` | Crear cola (TTL, max-length, dead letter opcionales) | 201, 400, 403, 404, 409 |
| GET | `/api/admin/queues` | Listar colas con Ready, Unacked y Consumers | 200 |
| GET | `/api/admin/queues/{nombre}` | Detalle de una cola | 200, 400, 404 |
| DELETE | `/api/admin/queues/{nombre}?soloSiVacia=&soloSiSinConsumidores=` | Eliminar cola | 204, 403, 404, 409 |
| DELETE | `/api/admin/queues/{nombre}/mensajes` | Purgar (limpieza) | 200, 404 |
| POST | `/api/admin/exchanges` | Crear exchange `DIRECT`, `TOPIC`, `FANOUT` o `HEADERS` | 201, 400, 403, 409 |
| GET | `/api/admin/exchanges` | Listar exchanges | 200 |
| DELETE | `/api/admin/exchanges/{nombre}` | Eliminar exchange | 204, 403, 404 |
| POST | `/api/admin/bindings` | Crear binding exchange → cola | 201, 400, 404, 409 |
| GET | `/api/admin/bindings` | Listar bindings | 200 |
| DELETE | `/api/admin/bindings` | Eliminar binding (mismo cuerpo que el POST) | 204, 400, 404 |
| PUT | `/api/admin/policies/{nombre}` | Crear o reemplazar política TTL / max-length | 200, 400 |
| GET | `/api/admin/policies` | Listar políticas | 200 |
| DELETE | `/api/admin/policies/{nombre}` | Eliminar política | 204, 404 |
| GET | `/api/admin/cluster/nodos` | Nodos del clúster y su estado | 200 |

Reglas de validación:
- Nombres de 3 a 100 caracteres (letras, números, `.`, `-`, `_`) que empiezan con letra o número.
- TTL y max-length positivos y con tope (7 días y 1.000.000).
- `deadLetterRoutingKey` exige `deadLetterExchange`.
- Una cola no puede ser `durable` y `autoDelete` a la vez.
- El tipo de exchange debe ser válido.
- La expresión regular de una política debe compilar.
- Los recursos `amq.*` están protegidos.

Ejemplo de error:
```json
{"status":400,"error":"Bad Request","mensaje":"Parámetros de entrada inválidos",
 "errores":{"nombre":"el nombre de la cola es obligatorio","ttlMs":"el TTL debe ser mayor a 0"}}
```

## Ejecución local

Requisitos: Docker Desktop. Para correr los servicios fuera de Docker también se necesita Java 21.

```bash
cp .env.example .env              # editar claves
docker compose up -d --build      # clúster + init + ambos microservicios
docker compose ps                 # rabbit-init debe terminar con "Exited (0)"
```

- Catálogo web: http://localhost:8080
- Swagger del administrador: http://localhost:8081/swagger-ui.html
- Management UI: http://localhost:15672 (nodo 1) y http://localhost:15673 (nodo 2)

Si antes se levantó una versión anterior con otras propiedades de cola, parta de cero con `docker compose down -v`.

**Pruebas** (no necesitan RabbitMQ; la de integración se omite si no hay broker en `localhost:5672`):
```bash
cd pedidos-service && ./mvnw test
cd rabbit-admin-service && ./mvnw test
```

## Despliegue en AWS (EC2)

1. **EC2**: Ubuntu 24.04, tipo `t3.medium` o superior (2 nodos RabbitMQ y 2 JVM no caben en 1 GB).
2. **Security Group**, entrada solo desde tu IP: 22 (SSH), 8080, 8081, 15672 y 15673.
3. Instalar Docker y clonar el repositorio:
   ```bash
   sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
   sudo usermod -aG docker ubuntu && newgrp docker
   git clone https://github.com/<usuario>/<repositorio>.git && cd <repositorio>
   cp .env.example .env && nano .env
   docker compose up -d --build
   ```
4. Verificar: `docker exec pedidos360-rabbit1 rabbitmqctl cluster_status` debe listar `rabbit@rabbit1` y `rabbit@rabbit2` como nodos en ejecución.
5. Ejecutar la demo desde la EC2 con `HOST=localhost ./scripts/demo.sh <bloque>`. Los navegadores usan `http://<IP-pública>:15672`.

## Guion del video EP4 → comandos

| Rótulo | Qué mostrar | Cómo |
|---|---|---|
| [EP4-01/02] | Dos nodos y estado del clúster | `./scripts/demo.sh ep4-01` + Management UI → *Overview → Nodes* |
| [EP4-03] | Docker Compose en la nube | `./scripts/demo.sh ep4-03` en la EC2 (mostrar la IP/consola de AWS) |
| [EP4-04/05/06] | 3 colas, 3 DLQ, direct y topic | `./scripts/demo.sh ep4-04` + pestañas *Queues* y *Exchanges* |
| [EP4-07] | Productor → exchange → cola → consumidor | `./scripts/demo.sh ep4-07` + `docker compose logs -f pedidos-service` |
| [EP4-08] | Fallo → reintentos → DLQ → log | `./scripts/demo.sh ep4-08` y `ep4-08b` (mensaje ilegible) |
| [EP4-09] | Administrador: crear/eliminar | `./scripts/demo.sh ep4-09`, luego `ep4-09b` (o Swagger) |
| [EP4-10] | Ready, Unacked, Consumers, DLQ | `./scripts/demo.sh ep4-10` + *Queues*. Para ver mensajes en las DLQ, usar `AUDITORIA_DLQ_HABILITADA=false` |
| [EP4-11] | TTL / max-length / limpieza | `./scripts/demo.sh ep4-11` + *Admin → Policies* |

No mostrar el archivo `.env` ni claves en el video.

## Seguridad

- Las credenciales se leen desde variables de entorno (`.env`, ignorado por Git). `.env.example` solo trae valores de ejemplo.
- Los valores por defecto de `application.yml` (`labuser` / `labpass`) son solo para el laboratorio local.
