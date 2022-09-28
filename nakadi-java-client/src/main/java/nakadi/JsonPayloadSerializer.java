package nakadi;

import java.util.List;

public class JsonPayloadSerializer implements PayloadSerializer {

    private final JsonSupport jsonSupport;

    public JsonPayloadSerializer(final JsonSupport jsonSupport) {
        this.jsonSupport = jsonSupport;
    }

    @Override
    public <T> byte[] toBytes(List<T> o) {
        return jsonSupport.toJsonBytesCompressed(o);
    }

    @Override
    public <T> Object transformEventRecord(EventRecord<T> eventRecord) {
        return jsonSupport.transformEventRecord(eventRecord);
    }

    @Override
    public String payloadMimeType() {
        return "application/json; charset=utf8";
    }

}
