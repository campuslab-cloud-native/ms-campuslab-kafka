# Changelog

Todos los cambios relevantes de este repositorio se documentan aquí.
El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el versionamiento sigue [SemVer](https://semver.org/lang/es/).

## [Sin publicar]

- Tópicos `bookings.events.DLT` y `audit.timeline.DLT`; `audit.timeline` pasa a `compact,delete` con 14 días de retención.
- API de administración: describir tópico, actualizar cleanup policy, describir consumer groups con lag, protección de tópicos oficiales y errores HTTP coherentes.

## [1.0.0] - 2026-11-22

Primera versión estable.

- Compose de Kafka con 3 ZooKeeper, 3 brokers, Kafka UI e inicializador de tópicos.
- Administrador REST de tópicos y consumer groups en `/api/v1/kafka`.
