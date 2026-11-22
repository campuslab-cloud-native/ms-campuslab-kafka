package ms_campuslab_kafka.presentation;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import ms_campuslab_kafka.domain.ConsumerGroupInfo;
import ms_campuslab_kafka.domain.KafkaAdminService;
import ms_campuslab_kafka.domain.TopicInfo;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping ("/api/v1/kafka/topics")
@RestController
@RequiredArgsConstructor
public class KafkaAdminController {
    private final KafkaAdminService kafkaAdminService;

    @GetMapping
    public List<String> listTopics() {
        return kafkaAdminService.listTopics();
    }

    @PostMapping
    public ResponseEntity<Void> createTopic(@Valid @RequestBody CreateTopicRequest request) {
        kafkaAdminService.createTopic(
            request.name(),
            request.partitions(),
            request.replicationFactor(),
            request.retentionDays(),
            request.cleanupPolicy()
        );
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping("/{topicName}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topicName) {
        kafkaAdminService.deleteTopic(topicName);
        return ResponseEntity.noContent().build();
    }

    /** Particiones, factor de replicación y configuración principal del tópico. */
    @GetMapping("/{name}")
    public TopicInfo describeTopic(@PathVariable String name) {
        return kafkaAdminService.describeTopic(name);
    }

    @GetMapping("/{name}/config")
    public Map<String, String> getTopicConfig(@PathVariable String name) {
        return kafkaAdminService.getTopicConfig(name);
    }

    @PatchMapping ("/{name}/retention")
    public ResponseEntity<Void> updateTopicConfig(@PathVariable String name, @RequestParam int days) {
        kafkaAdminService.updateRetention(name, days);
        return ResponseEntity.noContent().build();
    }

    /** Cambia la cleanup policy: delete, compact o compact,delete. */
    @PatchMapping("/{name}/cleanup-policy")
    public ResponseEntity<Void> updateCleanupPolicy(@PathVariable String name, @RequestParam String value) {
        kafkaAdminService.updateCleanupPolicy(name, value);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/consumer-groups")
    public List<String> listConsumerGroups() {
        return kafkaAdminService.listConsumerGroups();
    }

    /** Estado, miembros y lag (retraso) de un consumer group. */
    @GetMapping("/consumer-groups/{groupId}")
    public ConsumerGroupInfo describeConsumerGroup(@PathVariable String groupId) {
        return kafkaAdminService.describeConsumerGroup(groupId);
    }

}
