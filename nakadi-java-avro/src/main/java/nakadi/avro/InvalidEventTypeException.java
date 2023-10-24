package nakadi.avro;

public class InvalidEventTypeException extends RuntimeException {
    public InvalidEventTypeException(String msg) {
        super(msg);
    }
}
