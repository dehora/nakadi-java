package nakadi;

import java.util.Collection;
import java.util.stream.Collectors;

public class JsonPublishingBatchSerializer implements PublishingBatchSerializer {

    private final JsonSupport jsonSupport;

    public JsonPublishingBatchSerializer(final JsonSupport jsonSupport) {
        this.jsonSupport = jsonSupport;
    }

    @Override
    public <T> byte[] toBytes(SerializationContext context, Collection<T> events) {
        return jsonSupport.toJsonBytesCompressed(events.stream()
                .map(e -> new EventRecord<>(context.name(), e))
                .map(jsonSupport::transformEventRecord)
                .collect(Collectors.toList()));
    }

}
