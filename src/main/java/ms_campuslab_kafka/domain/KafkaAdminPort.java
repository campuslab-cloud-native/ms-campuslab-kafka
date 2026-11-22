package ms_campuslab_kafka.domain;

import java.util.List;
import java.util.Map;

public interface KafkaAdminPort {

    void createTopic(
        String topicName,
        int numPartitions,
        short replicationFactor
    );

    /** Crea un tópico con retención (en días) y cleanup policy explícitas. */
    void createTopic(
        String topicName,
        int numPartitions,
        short replicationFactor,
        int retentionDays,
        String cleanupPolicy
    );

    List<String> listTopics();

    void deleteTopic(String topicName);

    Map<String,String> getTopicConfig(String topicName);

    TopicInfo describeTopic(String topicName);

    void updateRetention(String topicName, int days);

    void updateCleanupPolicy(String topicName, String cleanupPolicy);

    List<String> listConsumerGroups();

    ConsumerGroupInfo describeConsumerGroup(String groupId);

}
