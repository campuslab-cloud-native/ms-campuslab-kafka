package ms_campuslab_kafka.presentation;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import ms_campuslab_kafka.domain.KafkaAdminService;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;



@RequestMapping ("/api/kafka/topics")
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
        kafkaAdminService.createTopic(request.name(), request.partitions(), request.replicationFactor());
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping("/{topicName}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topicName) {
        kafkaAdminService.deleteTopic(topicName);
        return ResponseEntity.noContent().build();
    }
    
    
}
