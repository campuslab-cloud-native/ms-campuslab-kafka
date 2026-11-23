package ms_campuslab_kafka.data;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAdminAdapterTest {

    @Test
    void createsTopicWithRetentionAndCleanupConfiguration() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        KafkaFuture<Void> completion = mock(KafkaFuture.class);
        AtomicReference<Collection<NewTopic>> createdTopics = new AtomicReference<>();
        when(adminClient.createTopics(anyCollection())).thenAnswer(invocation -> {
            createdTopics.set(invocation.getArgument(0));
            return createResult;
        });
        when(createResult.all()).thenReturn(completion);
        when(completion.get(30, TimeUnit.SECONDS)).thenReturn(null);

        new KafkaAdminAdapter(adminClient).createTopic("custom.events", 3, (short) 2, 4, "compact");

        NewTopic topic = createdTopics.get().iterator().next();
        assertEquals("custom.events", topic.name());
        assertEquals(3, topic.numPartitions());
        assertEquals((short) 2, topic.replicationFactor());
        assertEquals(String.valueOf(4L * 24 * 60 * 60 * 1000), topic.configs().get("retention.ms"));
        assertEquals("compact", topic.configs().get("cleanup.policy"));
        verify(adminClient).createTopics(anyCollection());
    }

    @Test
    void listsTopicsInSortedOrder() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        ListTopicsResult listResult = mock(ListTopicsResult.class);
        KafkaFuture<Set<String>> names = mock(KafkaFuture.class);
        when(adminClient.listTopics()).thenReturn(listResult);
        when(listResult.names()).thenReturn(names);
        when(names.get()).thenReturn(Set.of("z-topic", "a-topic", "m-topic"));

        assertEquals(
                java.util.List.of("a-topic", "m-topic", "z-topic"),
                new KafkaAdminAdapter(adminClient).listTopics()
        );
    }
}
