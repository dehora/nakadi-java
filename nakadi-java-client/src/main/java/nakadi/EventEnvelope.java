package nakadi;

public class EventEnvelope<T> {
    private final T data;
    private final EventMetadata metadata;

    public EventEnvelope(final T data, final EventMetadata metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    public T getData() {
        return data;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }
}
