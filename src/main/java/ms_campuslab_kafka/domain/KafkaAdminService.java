package ms_campuslab_kafka.domain;


import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

@Service
public class KafkaAdminService {

    private final KafkaAdminPort kafkaAdminPort;

    public KafkaAdminService(KafkaAdminPort kafkaAdminPort) {
        this.kafkaAdminPort = kafkaAdminPort;
    }

    public List<String> listTopics() {
        return kafkaAdminPort.listTopics();
    }

    public void createTopic(
            String name,
            int partitions,
            short replicationFactor
    ) {
        createTopic(name, partitions, replicationFactor, null, null);
    }

    /**
     * Crea un tópico. Si no se indican, la retención es de 7 días y la cleanup policy es "delete".
     */
    public void createTopic(
            String name,
            int partitions,
            short replicationFactor,
            Integer retentionDays,
            String cleanupPolicy
    ) {
        int days = retentionDays != null ? retentionDays : TopicPolicy.DEFAULT_RETENTION_DAYS;
        String policy = cleanupPolicy != null && !cleanupPolicy.isBlank()
                ? validateCleanupPolicy(cleanupPolicy)
                : TopicPolicy.DEFAULT_CLEANUP_POLICY;

        if (retentionDays != null) {
            validateRetention(name, days);
        }

        kafkaAdminPort.createTopic(
                name,
                partitions,
                replicationFactor,
                days,
                policy
        );
    }

    public void deleteTopic(String name) {
        if (TopicPolicy.isOfficial(name)) {
            throw new ProtectedTopicException(
                "The topic '" + name + "' belongs to the official CampusLab topology and cannot be deleted"
            );
        }
        kafkaAdminPort.deleteTopic(name);
    }

    public Map<String, String> getTopicConfig(String topicName) {
        return kafkaAdminPort.getTopicConfig(topicName);
    }

    public TopicInfo describeTopic(String topicName) {
        return kafkaAdminPort.describeTopic(topicName);
    }

    public void updateRetention(String topicName, int days) {
        validateRetention(topicName, days);
        kafkaAdminPort.updateRetention(topicName, days);
    }

    public void updateCleanupPolicy(String topicName, String cleanupPolicy) {
        kafkaAdminPort.updateCleanupPolicy(topicName, validateCleanupPolicy(cleanupPolicy));
    }

    public List<String> listConsumerGroups() {
        return kafkaAdminPort.listConsumerGroups();
    }

    public ConsumerGroupInfo describeConsumerGroup(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("The consumer group id must not be blank");
        }
        return kafkaAdminPort.describeConsumerGroup(groupId);
    }

    private void validateRetention(String topicName, int days) {
        int min = TopicPolicy.minRetentionDays(topicName);
        int max = TopicPolicy.maxRetentionDays(topicName);

        if (days < min || days > max) {
            throw new IllegalArgumentException(
                "The retention period for '" + topicName + "' must be between "
                    + min + " and " + max + " days"
            );
        }
    }

    private String validateCleanupPolicy(String cleanupPolicy) {
        String normalized = cleanupPolicy.trim().toLowerCase().replace(" ", "");

        if (!TopicPolicy.CLEANUP_POLICIES.contains(normalized)) {
            throw new IllegalArgumentException(
                "The cleanup policy must be one of: delete, compact, compact,delete"
            );
        }
        return normalized;
    }
}
