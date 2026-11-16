package ms_campuslab_kafka.data;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;
import ms_campuslab_kafka.domain.KafkaAdminPort;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Repository
@RequiredArgsConstructor 
public class KafkaAdminAdapter implements KafkaAdminPort {

    private final AdminClient adminClient;

    @Override
    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        throw new UnsupportedOperationException("createTopic not implemented yet");
    }

    @Override
    public List<String> listTopics() {
        try {
            return adminClient
                .listTopics()
                .names()
                .get()
                .stream()
                .sorted()
                .toList();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "Consult topics kafka interrupted", e
            );
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                "Consult topics kafka execution error", e
            );
        }
    }

    @Override
    public void deleteTopic(String topicName) {
        throw new UnsupportedOperationException("deleteTopic not implemented yet");
    }

}
