package ms_campuslab_kafka.data;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;
import ms_campuslab_kafka.domain.KafkaAdminPort;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Repository
@RequiredArgsConstructor
public class KafkaAdminAdapter implements KafkaAdminPort {

    private final AdminClient adminClient;

    @Override
    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        
        NewTopic topic = new NewTopic(
            topicName, numPartitions, replicationFactor
        );
        topic.configs(Map.of(
            "retention.ms", "604800000",
            "cleanup.policy", "delete"
        ));

        try {
            adminClient
            .createTopics(
                Collections
                .singleton(topic))
                .all()
                .get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "Create topic kafka interrupted", e
            );
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                "Create topic kafka execution error", e
            );
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException(
                "Create topic kafka timeout error", e
            );
        }
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
        try{
            adminClient.deleteTopics(
                Collections.singleton(topicName))
                .all()
                .get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "Delete topic kafka interrupted", e
            );
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                "Delete topic kafka execution error", e
            );
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException(
                "Delete topic kafka timeout error", e
            );
        }
    }

}
