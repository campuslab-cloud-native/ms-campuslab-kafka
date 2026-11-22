package ms_campuslab_kafka.domain;

import java.util.Map;

/** Descripción de un tópico: particiones, factor de replicación y configuración principal. */
public record TopicInfo(
        String name,
        int partitions,
        int replicationFactor,
        Map<String, String> configs
) {
}
