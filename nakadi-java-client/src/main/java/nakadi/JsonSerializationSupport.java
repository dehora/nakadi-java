package nakadi;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JsonSerializationSupport implements SerializationSupport {

    private JsonPublishingBatchSerializer payloadSerializer;
    private Map<String, SerializationContext> contextCache;

    public JsonSerializationSupport(JsonPublishingBatchSerializer payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        this.contextCache = new ConcurrentHashMap<>();
    }

    public static SerializationSupport newInstance(JsonSupport jsonSupport) {
        return new JsonSerializationSupport(new JsonPublishingBatchSerializer(jsonSupport));
    }

    @Override
    public <T> byte[] serializePayload(NakadiClient client, String eventTypeName, Collection<T> events) {
        SerializationContext context = contextCache.computeIfAbsent(eventTypeName, JsonSerializationContext::new);
        return payloadSerializer.toBytes(context, events);
    }

    @Override
    public String contentType() {
        return ResourceSupport.APPLICATION_JSON_CHARSET_UTF_8;
    }

    private static class JsonSerializationContext implements SerializationContext {

        private String eventTypeName;

        public JsonSerializationContext(String eventTypeName) {
            this.eventTypeName = eventTypeName;
        }

        @Override
        public String name() {
            return eventTypeName;
        }

        @Override
        public String schema() {
            throw new UnsupportedOperationException("Serialization Context does not use JSON schema");
        }

        @Override
        public String version() {
            throw new UnsupportedOperationException("Serialization Context does not use schema version");
        }
    }

    ;
}
