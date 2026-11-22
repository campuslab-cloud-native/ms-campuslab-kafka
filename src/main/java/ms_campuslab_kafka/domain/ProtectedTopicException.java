package ms_campuslab_kafka.domain;

/** Se lanza al intentar eliminar un tópico de la topología oficial de CampusLab. La API responde 409. */
public class ProtectedTopicException extends RuntimeException {

    public ProtectedTopicException(String message) {
        super(message);
    }
}
