package ms_campuslab_kafka.domain;


import java.util.List;

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
}
