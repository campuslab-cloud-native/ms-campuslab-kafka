package ms_campuslab_kafka.domain;

import java.util.List;
import java.util.Map;

public interface KafkaAdminPort {

    void createTopic(
        String topicName,
        int numPartitions,
        short replicationFactor
    );

    List<String> listTopics();

    void deleteTopic(String topicName);

    Map<String, String> getTopicConfig(String topicName);

    void updateTopicConfig(String topicName, int days);

}
