package ms_campuslab_kafka.presentation;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateTopicRequest (
    @NotBlank(message = "Topic name must not be blank")
    String name,

    @Min(value = 1, message = "Partitions must be at least 1")
    int partitions,

    @Min(value = 1, message = "Replication factor must be at least 1")
    @Max(value = 3, message = "Replication factor must be at most 3")
    short replicationFactor
) {}
