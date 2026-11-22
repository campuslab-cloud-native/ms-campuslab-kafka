package ms_campuslab_kafka.presentation;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Solicitud de creación de tópico. {@code retentionDays} y {@code cleanupPolicy} son opcionales:
 * si se omiten, el tópico se crea con 7 días de retención y la política "delete".
 */
public record CreateTopicRequest (
    @NotBlank(message = "Topic name must not be blank")
    @Pattern(
        regexp = "^[A-Za-z0-9._-]{1,249}$",
        message = "Topic name may only contain letters, numbers, '.', '_' and '-' (max 249 characters)"
    )
    String name,

    @Min(value = 1, message = "Partitions must be at least 1")
    int partitions,

    @Min(value = 1, message = "Replication factor must be at least 1")
    @Max(value = 3, message = "Replication factor must be at most 3")
    short replicationFactor,

    @Min(value = 1, message = "Retention must be at least 1 day")
    @Max(value = 30, message = "Retention must be at most 30 days")
    Integer retentionDays,

    String cleanupPolicy
) {}
