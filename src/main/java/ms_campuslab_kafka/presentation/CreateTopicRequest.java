package ms_campuslab_kafka.presentation;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CreateTopicRequest (
    @NotBlank(message = "Topic name must not be blank")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "Topic name can only contain alphanumeric characters, dots, underscores, and hyphens")
    String name,

    @Min(value = 1, message = "Partitions must be at least 1")
    int partitions,

    @Min(value = 1, message = "Replication factor must be at least 1")
    @Max(value = 3, message = "Replication factor must be at most 3")
    short replicationFactor
) {}
