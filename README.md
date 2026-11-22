# ms-campuslab-kafka

Infraestructura local Kafka y API administrativa de CampusLab. Kafka
complementa RabbitMQ; no lo sustituye.

## Qué está implementado

- `compose.yml` define tres nodos ZooKeeper, tres brokers Kafka, Kafka UI y el
  inicializador de tópicos.
- `KafkaAdminController` implementa el API HTTP administrativo. Se ejecuta
  como aplicación Spring Boot separada; no es un servicio del Compose Kafka.
- Bookings publica eventos en `bookings.events`.
- Audit consume `bookings.events` con el grupo `campuslab-audit` y publica
  eventos transformados en `audit.timeline`.
- No hay un consumidor de `audit.timeline` en los repositorios actuales.

La presencia de código y Compose no confirma que la topología esté activa en
AWS. Este repositorio no contiene un `compose.aws.yml`; la conectividad y el
estado de un despliegue EC2 deben verificarse en ese entorno.

## Topología del Compose local

| Componente | Dirección en la red Docker | Puerto publicado |
|---|---|---|
| `kafka1` | `kafka1:29092` | `9092` |
| `kafka2` | `kafka2:29092` | `9093` |
| `kafka3` | `kafka3:29092` | `9094` |
| Kafka UI | Navegador del host | `8080`, ligado por defecto a loopback |
| ZooKeeper | Tres nodos internos | No publica puertos al host |

El Compose usa Confluent Kafka/ZooKeeper `7.6.15`. Los brokers usan listeners
internos en `29092` y listeners externos publicados en `9092`–`9094`.
`KAFKA_HOST` determina el hostname anunciado externamente y por defecto es
`localhost`. Para aplicaciones en Docker, use las direcciones internas; para
clientes externos, configure un hostname resoluble y alcanzable, no una IP
fija en la documentación.

Los brokers tienen `KAFKA_MIN_INSYNC_REPLICAS=2`,
`KAFKA_DEFAULT_REPLICATION_FACTOR=3` y retención predeterminada de 168 horas.
Cuando `kafka-init` crea los tópicos del proyecto, configura:

| Tópico | Particiones | Réplicas | `cleanup.policy` | `retention.ms` |
|---|---:|---:|---|---:|
| `bookings.events` | 3 | 3 | `delete` | `604800000` |
| `audit.timeline` | 3 | 3 | `delete` | `604800000` |

`kafka-init` crea cada tópico solo si todavía no existe; no actualiza la
configuración de tópicos preexistentes.

## Flujos y contratos

| Flujo | Productor | Consumidor | Clave |
|---|---|---|---|
| `bookings.events` | `ms-campuslab-bookings` | `ms-campuslab-audit`, grupo `campuslab-audit` | `bookingId` |
| `audit.timeline` | `ms-campuslab-audit` | No hay consumidor en los repositorios actuales | `bookingId` |

Los eventos `BookingEvent` incluyen `eventId`, `type`, `bookingId`,
`resourceId`, `userId`, `timestamp`, `traceId` y `correlationId`. Los tipos
publicados son `BOOKING_CREATED`, `BOOKING_APPROVED`, `BOOKING_PREPARING`,
`BOOKING_IN_USE`, `BOOKING_RETURNED` y `BOOKING_CANCELLED`.

Audit deriva `AuditEvent` con `eventId`, `bookingId`, `userId`, `type`,
`timestamp`, `source`, `traceId` y `correlationId`. El contrato de mensajes se
mantiene en [`asyncapi.yaml`](./asyncapi.yaml). No hay tópicos DLT Kafka
configurados por estos servicios.

## API administrativo Kafka

El administrador corre en el puerto `8086` y expone `/api/kafka/topics`
(excepción al prefijo `/api/v1/` de las APIs de negocio):

- listar, crear y borrar tópicos;
- consultar su configuración y actualizar la retención entre 1 y 7 días;
- listar consumer groups.

Los detalles de métodos, parámetros y cuerpos están en
[`openapi.yaml`](./openapi.yaml). El controlador no implementa autenticación
HTTP propia; no exponer este API fuera de una red administrada.

## Ejecución local

Desde este repositorio:

```bash
docker compose config
docker compose up -d
docker compose ps
docker compose logs --tail=100
```

La red creada por el proyecto se llama `campuslab-kafka_kafka-net`; el Compose
de `infra-campuslab` la usa en el entorno local. Los servicios de aplicación
deben recibir `KAFKA_BOOTSTRAP_SERVERS` con hosts alcanzables desde su red.
Kafka, ZooKeeper y Kafka UI usan `restart: unless-stopped`; `kafka-init` es una
tarea de una sola ejecución (`restart: "no"`).

Para apagar el entorno local:

```bash
docker compose down
```

La aplicación administrativa se ejecuta por separado:

```bash
./mvnw spring-boot:run
```

Su propiedad `KAFKA_BOOTSTRAP_SERVERS` tiene por defecto los puertos locales
`9092`, `9093` y `9094`.
