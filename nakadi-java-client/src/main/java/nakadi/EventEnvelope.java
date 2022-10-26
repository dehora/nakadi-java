package nakadi;

import org.apache.avro.generic.GenericRecord;

public class EventEnvelope<T extends GenericRecord> implements Event<T>{
    private final T data;
    private final EventMetadata metadata;

    public EventEnvelope(final T data, final EventMetadata metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    public T data() {
        return data;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }
}
