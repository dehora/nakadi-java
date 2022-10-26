package nakadi;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class JsonPayloadSerializer implements PayloadSerializer {

    private final JsonSupport jsonSupport;

    public JsonPayloadSerializer(final JsonSupport jsonSupport) {
        this.jsonSupport = jsonSupport;
    }

    @Override
    public <T> byte[] toBytes(String eventTypeName, Collection<T> events) {
        List<EventRecord<T>> collect =
                events.stream().map(e -> new EventRecord<>(eventTypeName, e)).collect(Collectors.toList());
        List<Object> eventList =
                collect.stream().map(jsonSupport::transformEventRecord).collect(Collectors.toList());

        return jsonSupport.toJsonBytesCompressed(eventList);
    }

    @Override
    public String payloadMimeType() {
        return "application/json; charset=utf8";
    }

}
