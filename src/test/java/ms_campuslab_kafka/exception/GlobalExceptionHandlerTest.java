package ms_campuslab_kafka.exception;

import ms_campuslab_kafka.domain.ProtectedTopicException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void mapsProtectedTopicsToConflict() {
        ResponseEntity<Map<String, String>> response =
                handler.handleProtectedTopic(new ProtectedTopicException("protected"));

        assertResponse(response, HttpStatus.CONFLICT, "protected");
    }

    @Test
    void mapsMissingConsumerGroupsToNotFound() {
        ResponseEntity<Map<String, String>> response =
                handler.handleGroupNotFound(new GroupIdNotFoundException("missing"));

        assertResponse(response, HttpStatus.NOT_FOUND, "missing");
    }

    @Test
    void classifiesKafkaFailuresByNestedCause() {
        ResponseEntity<Map<String, String>> response =
                handler.handleKafkaFailure(new IllegalStateException("operation failed", new TopicExistsException("exists")));

        assertResponse(response, HttpStatus.CONFLICT, "The topic already exists");
    }

    @Test
    void mapsMissingTopicsAndInvalidTopicParameters() {
        ResponseEntity<Map<String, String>> missingTopic =
                handler.handleKafkaFailure(new IllegalStateException(
                        "operation failed",
                        new UnknownTopicOrPartitionException("missing")
                ));
        ResponseEntity<Map<String, String>> invalidTopic =
                handler.handleKafkaFailure(new IllegalStateException(
                        "operation failed",
                        new InvalidTopicException("invalid")
                ));

        assertResponse(missingTopic, HttpStatus.NOT_FOUND, "The topic does not exist");
        assertResponse(invalidTopic, HttpStatus.BAD_REQUEST, "Kafka rejected the requested topic parameters");
    }

    @Test
    void mapsIllegalArgumentsToBadRequest() {
        ResponseEntity<Map<String, String>> response =
                handler.handleIllegalArgument(new IllegalArgumentException("bad input"));

        assertResponse(response, HttpStatus.BAD_REQUEST, "bad input");
    }

    @Test
    void returnsGenericServerErrorForUnexpectedFailures() {
        ResponseEntity<Map<String, String>> response =
                handler.handleUnexpected(new Exception("internal detail"));

        assertResponse(response, HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error");
    }

    private void assertResponse(
            ResponseEntity<Map<String, String>> response,
            HttpStatus status,
            String message
    ) {
        assertEquals(status, response.getStatusCode());
        assertEquals(Map.of("message", message), response.getBody());
    }
}
