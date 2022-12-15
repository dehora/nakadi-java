package nakadi.avro;

public class InvalidSchemaException extends RuntimeException {
    public InvalidSchemaException(String msg) {
        super(msg);
    }
}
