package ms_campuslab_kafka.presentation;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import ms_campuslab_kafka.domain.KafkaAdminService;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;


@RequestMapping ("/api/kafka/topics")
@RestController 
@RequiredArgsConstructor 
public class KafkaAdminController {
    private final KafkaAdminService kafkaAdminService;

    @GetMapping
    public List<String> listTopics() {
        return kafkaAdminService.listTopics();
    }
    
}
