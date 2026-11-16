# ms-campuslab-kafka

Infraestructura y contrato de eventos de **Apache Kafka para CampusLab**, correspondiente a la Evaluación Parcial N.º 5 de Desarrollo Cloud Native I (DSY1107). Kafka complementa la mensajería ya implementada con RabbitMQ; **no la sustituye**.

> **Estado:** documentación de la arquitectura objetivo, no evidencia de despliegue. Aún se deben implementar y verificar `compose.yml`, el administrador Kafka y las integraciones Spring Boot. El contrato está sujeto a cotejo con los DTO del código.

## Alcance solicitado por la pauta

- Ejecutar Kafka con **ZooKeeper, brokers y Kafka UI** mediante Docker Compose.
- Configurar clientes Kafka de Spring Boot (`bootstrap.servers`, serializers/deserializers y `group.id`).
- Implementar **productores y consumidores** que publiquen y lean eventos tipados en los dos tópicos definidos.
- Administrar tópicos: creación, listado, modificación de configuraciones y eliminación; complementar con inspección de grupos consumidores mediante herramientas o scripts.
- Gestionar errores con validación del payload, logs contextualizados y reintentos controlados.
- Mantener operativos los productores, consumidores y DLQ de RabbitMQ ya solicitados en la evaluación anterior.

## Topología propuesta

```text
Microservicios Spring Boot
  |-- RabbitMQ: comandos, colas, DLQ (flujo existente)
  `-- Kafka: eventos persistidos por tópico
        |-- bookings.events
        `-- audit.timeline
                |
                `-- Kafka UI / Kafka Admin (administración)

Cluster Kafka (Docker Compose): 3 brokers + 3 nodos ZooKeeper + Kafka UI
```

**Despliegue AWS propuesto:** una EC2 dedicada a Kafka y sus contenedores, distinta de la EC2 de RabbitMQ. Esto es una decisión de arquitectura del proyecto, **no un requisito explícito de la pauta**. Tres brokers en una única EC2 permiten ensayar replicación entre brokers, pero no sobreviven a la caída de esa EC2. Limitar acceso a brokers a la red privada y configurar `advertised.listeners` alcanzables desde la EC2 de aplicaciones.

## Tópicos y retención

La retención será de **7 días como máximo** en todos los tópicos de datos definidos. Para el diseño actual se fija exactamente en 7 días (`604800000` ms), sin los rangos del README anterior.

| Tópico | Particiones | Réplicas | `cleanup.policy` | `retention.ms` |
|---|---:|---:|---|---:|
| `bookings.events` | 3 | 3 | `delete` | `604800000` |
| `audit.timeline` | 3 | 3 | `delete` | `604800000` |

**Nota sobre auditoría:** el README original indicaba `compact,delete` para `audit.timeline`. Se propone `delete` para conservar una línea temporal de eventos (la compactación podría eliminar registros con la misma clave). Confirmar esta decisión con los requisitos de negocio antes de crear el tópico.

**DLT en Kafka:** la pauta exige manejo de errores, logging y reintentos, pero **no exige expresamente Dead Letter Topics Kafka**. Por ahora no se agregan DLT al contrato obligatorio. Si el equipo decide implementarlos, documentar `bookings.events.DLT` y/o `audit.timeline.DLT` con un máximo de 7 días de retención y definir formalmente su payload y política de recuperación. Las **DLQ RabbitMQ sí son obligatorias** y deben seguir funcionando.

El factor de replicación 3 requiere tres brokers disponibles para crear los tópicos. La clave propuesta para los eventos de reserva es `bookingId` (serializada consistentemente) para mantener la secuencia de eventos de una misma reserva dentro de una partición; Kafka **no garantiza orden global entre particiones**.

## Contrato AsyncAPI

El archivo [`asyncapi.yaml`](./asyncapi.yaml) describe los siguientes canales:

- `bookings.events`: `BookingEvent` con `eventId`, `type`, `bookingId`, `timestamp`; campos opcionales `resourceId`, `userId`, `traceId`, `correlationId`.
- `audit.timeline`: `AuditEvent` con `eventId`, `bookingId`, `type`, `timestamp`; campos opcionales `userId`, `source`, `traceId`, `correlationId`.

Tipos conocidos de `BookingEvent`: `BOOKING_CREATED`, `BOOKING_APPROVED`, `BOOKING_PREPARING`, `BOOKING_IN_USE`, `BOOKING_RETURNED`, `BOOKING_CANCELLED`.

El contrato define publicación/consumo desde el punto de vista de la aplicación descrita. La asignación exacta de qué servicio produce o consume cada canal se confirmará revisando las clases Java.

## Consumer groups

Un **consumer group** permite que varias instancias del mismo consumidor compartan el procesamiento de las particiones de un tópico. Dos grupos distintos pueden leer independientemente el mismo evento. Los offsets son gestionados por grupo y partición.

**Convención propuesta, pendiente de implementar:**

| Flujo | Grupo sugerido | Propósito |
|---|---|---|
| `bookings.events` → auditoría | `campuslab-audit` | Registrar cambios de reserva |
| `audit.timeline` → consumidor de auditoría (si corresponde) | `campuslab-audit-timeline` | Consultar/procesar la línea temporal |

No es necesario tener tres instancias activas: las tres particiones establecen el máximo de consumidores que pueden procesar simultáneamente *dentro de un mismo grupo* para ese tópico. No afirmar que estos grupos existen hasta verificarlos en el clúster.

Ejemplo de configuración para un consumidor **(ilustrativo, no copiado del proyecto)**:

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: campuslab-audit
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "<paquete.de.dto.del.proyecto>"
```

Configurar el serializador del productor en el código del microservicio; el `group-id` del ejemplo debe ajustarse para cada caso de uso. Nunca utilizar `trusted.packages: '*'` en producción.

## Conectividad y ejecución

- `localhost:9092` en AsyncAPI es **solo un ejemplo para desarrollo local**. Fuera del host, `bootstrap.servers` debe apuntar a los brokers y puertos accesibles desde las aplicaciones.
- Los brokers deben anunciar listeners reales (`advertised.listeners`) que se resuelvan desde la red de las aplicaciones.
- Kafka UI permite inspeccionar brokers, tópicos, particiones y grupos de consumidores; el administrador Spring Boot debe ofrecer las operaciones requeridas por la pauta.

Una vez que exista `compose.yml` en este repositorio:

```bash
docker compose config
docker compose up -d
docker compose ps
docker compose logs --tail=100
# Al terminar:
docker compose down
```

Los comandos **no son evidencia de que el compose exista o funcione todavía**.

## Lista de verificación — Evaluación Parcial N.º 5

- [ ] `compose.yml` funcional con ZooKeeper, 3 brokers y Kafka UI.
- [ ] Creación de ambos tópicos, 3 particiones, factor 3, `delete`, `retention.ms=604800000`.
- [ ] Clientes Spring Boot conectan con brokers; serializers, deserializers y grupos correctos.
- [ ] Productores con DTO y eventos coherentes con `asyncapi.yaml`.
- [ ] Consumidores (`@KafkaListener` o API equivalente), grupos y lógica de negocio separados.
- [ ] Manejo de payloads inválidos, logs, reintentos y pruebas ante fallos (DLT Kafka opcional).
- [ ] Administrador Kafka: crear, listar, modificar y eliminar tópicos/configuraciones.
- [ ] Inspección reproducible de tópicos y consumer groups mediante scripts o administrador.
- [ ] Pruebas de publicación/consumo en ambos tópicos y revisión de offsets.
- [ ] RabbitMQ y sus DLQ continúan operativos.
- [ ] DTO reales, `bootstrap.servers`, redes y puertos coinciden con el contrato y Compose.

## Referencias proporcionadas

- `EP5_DSY1107_Estudiante_encargo.pdf` — instrucciones y rúbrica.
- `Guía_Práctica_de_Apache_Kafka_y_Spring_Boot.pdf` — productores, consumidores, particiones y offsets.
- `Kafka Streams.pdf` — procesamiento continuo y topologías (material de apoyo, no obligación explícita de implementar Kafka Streams).
- `Arquitectura_de_Apache_Kafka.pdf` — brokers, replicación, grupos y retención.
