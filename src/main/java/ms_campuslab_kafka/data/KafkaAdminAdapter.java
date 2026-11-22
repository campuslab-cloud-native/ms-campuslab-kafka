package ms_campuslab_kafka.data;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;
import ms_campuslab_kafka.domain.ConsumerGroupInfo;
import ms_campuslab_kafka.domain.KafkaAdminPort;
import ms_campuslab_kafka.domain.TopicInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Repository
@RequiredArgsConstructor
public class KafkaAdminAdapter implements KafkaAdminPort {

    private static final long DAY_MS = 24L * 60 * 60 * 1000;

    private final AdminClient adminClient;

    @Override
    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        createTopic(topicName, numPartitions, replicationFactor, 7, "delete");
    }

    @Override
    public void createTopic(
            String topicName,
            int numPartitions,
            short replicationFactor,
            int retentionDays,
            String cleanupPolicy
    ) {

        NewTopic topic = new NewTopic(
            topicName, numPartitions, replicationFactor
        );
        topic.configs(Map.of(
            "retention.ms", String.valueOf(retentionDays * DAY_MS),
            "cleanup.policy", cleanupPolicy
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

    @Override
    public Map<String, String> getTopicConfig(String topicName) {

        ConfigResource configResource = new ConfigResource(
            ConfigResource.Type.TOPIC,
            topicName
        );

        try {
            var config = adminClient
            .describeConfigs(List.of(configResource)
            ).all()
            .get(30, TimeUnit.SECONDS)
            .get(configResource);

            return Map.of(
                "retention.ms", config.get("retention.ms").value(),
                "cleanup.policy", config.get("cleanup.policy").value()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "Get topic config kafka interrupted", e
            );
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                "Get topic config kafka execution error", e
            );
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException(
                "Get topic config kafka timeout error", e
            );
        }
    }

    @Override
    public TopicInfo describeTopic(String topicName) {

        TopicDescription description = await(
            adminClient.describeTopics(List.of(topicName)).allTopicNames(),
            "Describe topic kafka"
        ).get(topicName);

        int replicationFactor = description.partitions().isEmpty()
            ? 0
            : description.partitions().get(0).replicas().size();

        return new TopicInfo(
            topicName,
            description.partitions().size(),
            replicationFactor,
            getTopicConfig(topicName)
        );
    }

    @Override
    public void updateRetention(String topicName, int days) {
        alterTopicConfig(
            topicName,
            "retention.ms",
            String.valueOf(days * DAY_MS)
        );
    }

    @Override
    public void updateCleanupPolicy(String topicName, String cleanupPolicy) {
        alterTopicConfig(topicName, "cleanup.policy", cleanupPolicy);
    }

    private void alterTopicConfig(String topicName, String key, String value) {

        ConfigResource resource = new ConfigResource(
            ConfigResource.Type.TOPIC,
            topicName
        );

        AlterConfigOp operation = new AlterConfigOp(
            new ConfigEntry(key, value),
            AlterConfigOp.OpType.SET
        );

        await(
            adminClient.incrementalAlterConfigs(
                Map.of(resource, List.of(operation))
            ).all(),
            "Update topic config kafka"
        );
    }

    @Override
    public List<String> listConsumerGroups() {
        try {
            return adminClient
                    .listGroups(ListGroupsOptions.forConsumerGroups())
                    .all()
                    .get(30, TimeUnit.SECONDS)
                    .stream()
                    .map(group -> group.groupId())
                    .sorted()
                    .toList();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Consult consumer groups kafka interrupted", e
            );
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Consult consumer groups kafka execution error", e
            );
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException(
                    "Consult consumer groups kafka timeout error", e
            );
        }
    }

    @Override
    public ConsumerGroupInfo describeConsumerGroup(String groupId) {

        ConsumerGroupDescription description = await(
            adminClient.describeConsumerGroups(List.of(groupId)).all(),
            "Describe consumer group kafka"
        ).get(groupId);

        Map<TopicPartition, OffsetAndMetadata> committed = await(
            adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
            "Consult consumer group offsets kafka"
        );

        String state = String.valueOf(description.groupState());

        if (description.members().isEmpty()
                && committed.isEmpty()
                && "Dead".equalsIgnoreCase(state)) {
            throw new GroupIdNotFoundException(
                "The consumer group '" + groupId + "' does not exist"
            );
        }

        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        committed.keySet().forEach(partition -> latest.put(partition, OffsetSpec.latest()));

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = latest.isEmpty()
            ? Map.of()
            : await(adminClient.listOffsets(latest).all(), "Consult end offsets kafka");

        Map<String, Long> lagByPartition = new TreeMap<>();
        long totalLag = 0;

        for (var entry : committed.entrySet()) {
            var end = endOffsets.get(entry.getKey());
            long lag = end == null || entry.getValue() == null
                ? 0
                : Math.max(0, end.offset() - entry.getValue().offset());

            lagByPartition.put(entry.getKey().toString(), lag);
            totalLag += lag;
        }

        return new ConsumerGroupInfo(
            groupId,
            state,
            description.members().size(),
            totalLag,
            lagByPartition
        );
    }

    private <T> T await(KafkaFuture<T> future, String action) {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(action + " interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(action + " execution error", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException(action + " timeout error", e);
        }
    }

}
