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
        kafkaAdminPort.createTopic(
                name,
                partitions,
                replicationFactor
        );
    }

    public void deleteTopic(String name) {
        kafkaAdminPort.deleteTopic(name);
    }

    public Map<String, String> getTopicConfig(String topicName) {
        return kafkaAdminPort.getTopicConfig(topicName);
    }

    public void updateRetention(String topicName, int days) {

        if(days < 1 || days > 7) {
            throw new IllegalArgumentException(
                "The retention period must be between 1 and 7 days"
            );
        }
        kafkaAdminPort.updateRetention(topicName, days);
    }
}
