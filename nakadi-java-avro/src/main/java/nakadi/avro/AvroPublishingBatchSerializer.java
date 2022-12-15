package nakadi.avro;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import nakadi.BusinessEventMapped;
import nakadi.DataChangeEvent;
import nakadi.EventMetadata;
import nakadi.PublishingBatchSerializer;
import nakadi.SerializationContext;
import org.apache.avro.Schema;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.generated.avro.PublishingBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The serializer uses jackson extension to serialize business pojos to avro events.
 */
public class AvroPublishingBatchSerializer implements PublishingBatchSerializer {

    private AvroMapper avroMapper;
    private Map<String, ObjectWriter> objectWriterCache;

    public AvroPublishingBatchSerializer(AvroMapper avroMapper) {
        this.avroMapper = avroMapper;
        this.objectWriterCache = new ConcurrentHashMap<>();
    }

    @Override
    public <T> byte[] toBytes(SerializationContext context, Collection<T> events) {
        try {
            List<Envelope> envelops = events.stream()
                    .map(event -> toEnvelope(context, event))
                    .collect(Collectors.toList());
            return PublishingBatch.newBuilder().setEvents(envelops)
                    .build().toByteBuffer().array();
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    private <T> Envelope toEnvelope(SerializationContext context, T event) {
        try {
            EventMetadata metadata;
            Object data;
            if (event instanceof BusinessEventMapped) {
                metadata = ((BusinessEventMapped) event).metadata();
                data = ((BusinessEventMapped) event).data();
            } else if (event instanceof DataChangeEvent) {
                metadata = ((DataChangeEvent) event).metadata();
                data = ((DataChangeEvent) event).data();
            } else {
                throw new InvalidEventTypeException("Unexpected event category `" +
                        event.getClass() + "` provided during avro serialization");
            }

            byte[] eventBytes = objectWriterCache.computeIfAbsent(context.name(),
                    (et) -> avroMapper.writer(new AvroSchema(new Schema.Parser().parse(context.schema()))))
                    .writeValueAsBytes(data);

            return Envelope.newBuilder()
                    .setMetadata(Metadata.newBuilder()
                            .setEventType(context.name()) // metadata.eventType ?
                            .setVersion(context.version())
                            .setOccurredAt(metadata.occurredAt().toInstant())
                            .setEid(metadata.eid())
                            .setPartition(metadata.partition())
                            .setPartitionCompactionKey(metadata.partitionCompactionKey())
                            .build())
                    .setPayload(ByteBuffer.wrap(eventBytes))
                    .build();
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

}
