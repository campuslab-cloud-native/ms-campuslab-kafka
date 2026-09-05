# ms-campuslab-kafka

Infraestructura Kafka de CampusLab.

## Tecnologías

- Apache Kafka
- Zookeeper
- Kafka UI
- Docker
- Docker Compose

## Infraestructura

```text
3 Kafka brokers
3 Zookeeper nodes
Kafka UI
```

## Tópicos

```text
bookings.events
audit.timeline
*.DLT
```

## bookings.events

```text
Partitions: 3
Replicas: 3
Policy: delete
Retention: 3-7 días
```

## audit.timeline

```text
Partitions: 3
Replicas: 3
Policy: compact,delete
Retention: 14-30 días
```

## Dead Letter Topics

```text
*.DLT
```

Configuración:

```text
Partitions: 3
Replicas: 3
Policy: delete
Retention: 7-14 días
```

## Puerto

```text
9092
```

## Ejecución

```bash
docker compose up -d
```

## Detener

```bash
docker compose down
```
