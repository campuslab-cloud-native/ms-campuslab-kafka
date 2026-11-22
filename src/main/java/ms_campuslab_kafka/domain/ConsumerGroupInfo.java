package ms_campuslab_kafka.domain;

import java.util.Map;

/** Estado de un consumer group y su retraso (lag) por partición. */
public record ConsumerGroupInfo(
        String groupId,
        String state,
        int members,
        long totalLag,
        Map<String, Long> lagByPartition
) {
}
