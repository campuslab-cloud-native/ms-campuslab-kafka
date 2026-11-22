package ms_campuslab_kafka.domain;

import java.util.Set;

/**
 * Reglas de la topología oficial de Kafka de CampusLab: qué tópicos están protegidos
 * y qué rango de retención (en días) corresponde a cada flujo.
 */
public final class TopicPolicy {

    public static final String BOOKINGS_EVENTS = "bookings.events";
    public static final String AUDIT_TIMELINE = "audit.timeline";
    public static final String DLT_SUFFIX = ".DLT";

    public static final int DEFAULT_RETENTION_DAYS = 7;
    public static final String DEFAULT_CLEANUP_POLICY = "delete";

    public static final Set<String> CLEANUP_POLICIES = Set.of("delete", "compact", "compact,delete");

    private static final Set<String> OFFICIAL_TOPICS = Set.of(
            BOOKINGS_EVENTS,
            AUDIT_TIMELINE,
            BOOKINGS_EVENTS + DLT_SUFFIX,
            AUDIT_TIMELINE + DLT_SUFFIX
    );

    private TopicPolicy() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean isOfficial(String topic) {
        return OFFICIAL_TOPICS.contains(topic);
    }

    /** Retención mínima permitida, en días. */
    public static int minRetentionDays(String topic) {
        if (BOOKINGS_EVENTS.equals(topic)) {
            return 3;
        }
        if (AUDIT_TIMELINE.equals(topic)) {
            return 14;
        }
        if (topic != null && topic.endsWith(DLT_SUFFIX)) {
            return 7;
        }
        return 1;
    }

    /** Retención máxima permitida, en días. */
    public static int maxRetentionDays(String topic) {
        if (BOOKINGS_EVENTS.equals(topic)) {
            return 7;
        }
        if (AUDIT_TIMELINE.equals(topic)) {
            return 30;
        }
        if (topic != null && topic.endsWith(DLT_SUFFIX)) {
            return 14;
        }
        return 30;
    }
}
