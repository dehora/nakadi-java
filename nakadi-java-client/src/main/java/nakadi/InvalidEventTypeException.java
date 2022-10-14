package nakadi;

public class InvalidEventTypeException extends RuntimeException {

    public InvalidEventTypeException(String message) {
        super(message);
    }
}
