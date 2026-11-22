package ms_campuslab_kafka.presentation;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import ms_campuslab_kafka.domain.ProtectedTopicException;

/**
 * Traduce los errores de Kafka y de validación a respuestas HTTP con un cuerpo {"message": "..."}.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> handleValidation(MethodArgumentNotValidException exception) {
        String message = exception.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getDefaultMessage())
                .distinct()
                .collect(Collectors.joining("; "));
        return response(HttpStatus.BAD_REQUEST, message.isBlank() ? "Invalid request" : message);
    }

    @ExceptionHandler({
        HttpMessageNotReadableException.class,
        MissingServletRequestParameterException.class,
        MethodArgumentTypeMismatchException.class
    })
    public ResponseEntity<Map<String, String>> handleBadRequest(Exception exception) {
        return response(HttpStatus.BAD_REQUEST, "The request is malformed or has missing or invalid parameters");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleIllegalArgument(IllegalArgumentException exception) {
        return response(HttpStatus.BAD_REQUEST, exception.getMessage());
    }

    @ExceptionHandler(ProtectedTopicException.class)
    public ResponseEntity<Map<String, String>> handleProtectedTopic(ProtectedTopicException exception) {
        return response(HttpStatus.CONFLICT, exception.getMessage());
    }

    @ExceptionHandler(GroupIdNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleGroupNotFound(GroupIdNotFoundException exception) {
        return response(HttpStatus.NOT_FOUND, exception.getMessage());
    }

    /** Errores de Kafka envueltos por el adaptador: se clasifican según su causa. */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> handleKafkaFailure(IllegalStateException exception) {

        if (hasCause(exception, TopicExistsException.class)) {
            return response(HttpStatus.CONFLICT, "The topic already exists");
        }
        if (hasCause(exception, UnknownTopicOrPartitionException.class)) {
            return response(HttpStatus.NOT_FOUND, "The topic does not exist");
        }
        if (hasCause(exception, GroupIdNotFoundException.class)) {
            return response(HttpStatus.NOT_FOUND, "The consumer group does not exist");
        }
        if (hasCause(exception, InvalidTopicException.class)
                || hasCause(exception, InvalidPartitionsException.class)
                || hasCause(exception, InvalidReplicationFactorException.class)
                || hasCause(exception, InvalidConfigurationException.class)) {
            return response(HttpStatus.BAD_REQUEST, "Kafka rejected the requested topic parameters");
        }

        log.error("Kafka operation failed", exception);
        return response(HttpStatus.BAD_GATEWAY, "The Kafka operation failed or Kafka is not available");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleUnexpected(Exception exception) {
        log.error("Unexpected error", exception);
        return response(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error");
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable current = throwable;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static ResponseEntity<Map<String, String>> response(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(Map.of("message", message));
    }
}
