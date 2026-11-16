
package ms_campuslab_kafka.domain;

import java.util.List;
public interface KafkaAdminPort {

    void createTopic(
        String topicName,
        int numPartitions,
        short replicationFactor
    );

    List<String> listTopics();

    void deleteTopic(String topicName);

}
