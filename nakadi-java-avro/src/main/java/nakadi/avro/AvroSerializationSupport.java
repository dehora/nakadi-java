package nakadi.avro;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import nakadi.EventType;
import nakadi.EventTypeSchema;
import nakadi.NakadiClient;
import nakadi.SerializationContext;
import nakadi.SerializationSupport;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AvroSerializationSupport implements SerializationSupport {

    private final AvroPublishingBatchSerializer payloadSerializer;
    private final Map<String, SerializationContext> contextCache;

    public AvroSerializationSupport(AvroPublishingBatchSerializer payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        this.contextCache = new ConcurrentHashMap<>();
    }

    public static SerializationSupport newInstance() {
        return new AvroSerializationSupport(new AvroPublishingBatchSerializer(new AvroMapper()));
    }

    @Override
    public <T> byte[] serializePayload(NakadiClient client, String eventTypeName, Collection<T> events) {
        SerializationContext context = contextCache.computeIfAbsent(
                eventTypeName, (et) -> new AvroSerializationContext(
                        client.resources().eventTypes().findByName(et)));
        return payloadSerializer.toBytes(context, events);
    }

    @Override
    public String contentType() {
        return "application/avro-binary";
    }

    private static class AvroSerializationContext implements SerializationContext {

        private final EventType eventType;

        private AvroSerializationContext(EventType eventType) {
            if (eventType.schema().type() != EventTypeSchema.Type.avro_schema) {
                throw new InvalidSchemaException(String.format(
                        "Event type `%s` schema is `%s`, but expected Avro",
                        eventType.name(), eventType.schema().type()));
            }

            this.eventType = eventType;
        }

        @Override
        public String name() {
            return eventType.name();
        }

        @Override
        public String schema() {
            return eventType.schema().schema();
        }

        @Override
        public String version() {
            return eventType.schema().version();
        }

    }
}
