package ms_campuslab_kafka.domain;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KafkaAdminServiceTest {

    private final KafkaAdminPort port = mock(KafkaAdminPort.class);
    private final KafkaAdminService service = new KafkaAdminService(port);

    @Test
    void createsTopicsWithDefaultRetentionAndCleanupPolicy() {
        service.createTopic("custom.events", 3, (short) 2);

        verify(port).createTopic("custom.events", 3, (short) 2, 7, "delete");
    }

    @Test
    void normalizesCleanupPolicyAndValidatesRetention() {
        service.createTopic("bookings.events", 4, (short) 2, 5, " COMPACT, DELETE ");

        verify(port).createTopic("bookings.events", 4, (short) 2, 5, "compact,delete");
    }

    @Test
    void rejectsRetentionOutsideTopicPolicyRange() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createTopic("bookings.events", 1, (short) 1, 2, null)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateRetention("audit.timeline", 31)
        );

        verifyNoInteractions(port);
    }

    @Test
    void rejectsUnsupportedCleanupPolicy() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateCleanupPolicy("custom.events", "compact,delete,delete")
        );

        verifyNoInteractions(port);
    }

    @Test
    void protectsOfficialTopicsFromDeletion() {
        assertThrows(
                ProtectedTopicException.class,
                () -> service.deleteTopic(TopicPolicy.BOOKINGS_EVENTS)
        );

        verify(port, never()).deleteTopic(TopicPolicy.BOOKINGS_EVENTS);
    }

    @Test
    void delegatesTopicAndConsumerGroupQueries() {
        TopicInfo topicInfo = new TopicInfo("custom.events", 2, 1, Map.of("cleanup.policy", "delete"));
        ConsumerGroupInfo groupInfo = new ConsumerGroupInfo("group-a", "Stable", 1, 3, Map.of());
        when(port.listTopics()).thenReturn(List.of("custom.events"));
        when(port.describeTopic("custom.events")).thenReturn(topicInfo);
        when(port.describeConsumerGroup("group-a")).thenReturn(groupInfo);
        when(port.getTopicConfig("custom.events")).thenReturn(Map.of("cleanup.policy", "delete"));
        when(port.listConsumerGroups()).thenReturn(List.of("group-a"));

        assertEquals(List.of("custom.events"), service.listTopics());
        assertEquals(topicInfo, service.describeTopic("custom.events"));
        assertEquals(groupInfo, service.describeConsumerGroup("group-a"));
        assertEquals(Map.of("cleanup.policy", "delete"), service.getTopicConfig("custom.events"));
        assertEquals(List.of("group-a"), service.listConsumerGroups());

        verify(port).listTopics();
        verify(port).describeTopic("custom.events");
        verify(port).describeConsumerGroup("group-a");
        verify(port).getTopicConfig("custom.events");
        verify(port).listConsumerGroups();
    }

    @Test
    void delegatesValidConfigurationUpdates() {
        service.updateRetention("custom.events", 14);
        service.updateCleanupPolicy("custom.events", " COMPACT ");

        verify(port).updateRetention("custom.events", 14);
        verify(port).updateCleanupPolicy("custom.events", "compact");
    }

    @Test
    void rejectsBlankConsumerGroupBeforeDelegation() {
        assertThrows(IllegalArgumentException.class, () -> service.describeConsumerGroup("  "));

        verifyNoInteractions(port);
    }
}
