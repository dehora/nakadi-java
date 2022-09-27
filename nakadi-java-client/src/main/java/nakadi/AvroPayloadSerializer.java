package nakadi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.generated.avro.PublishingBatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class AvroPayloadSerializer implements PayloadSerializer {

    private final String eventType;
    private final String schemaVersion;
    private final Schema schema;

    public AvroPayloadSerializer(final String eventType,
                                 final String schemaVersion,
                                 final Schema schema) {
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.schema = schema;
    }

    @Override
    public <T> byte[] toBytes(final List<T> events) {
        try {
            final List<Envelope> envelops = events.stream()
                    .map(event -> {
                        EventEnvelope realEvent = (EventEnvelope) event;
                        try {
                            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            new GenericDatumWriter(schema).write(realEvent.getData(),
                                    EncoderFactory.get().directBinaryEncoder(baos, null));
                            final EventMetadata metadata = realEvent.getMetadata();

                            return Envelope.newBuilder()
                                    .setMetadata(Metadata.newBuilder()
                                            .setEventType(metadata.eventType())
                                            .setVersion(schemaVersion)
                                            .setOccurredAt(metadata.occurredAt().toInstant())
                                            .setEid(metadata.eid())
                                            .setPartition(metadata.partition())
                                            .setPartitionCompactionKey(metadata.partitionCompactionKey())
                                            .build())
                                    .setPayload(ByteBuffer.wrap(baos.toByteArray()))
                                    .build();
                        } catch (IOException io) {
                            throw new RuntimeException();
                        }
                    })
                    .collect(Collectors.toList());
            return PublishingBatch.newBuilder().setEvents(envelops)
                    .build().toByteBuffer().array();
        } catch (IOException io) {
            throw new RuntimeException();
        }
    }

    @Override
    public <T> Object transformEventRecord(EventRecord<T> eventRecord) {
        return eventRecord.event();
    }

    @Override
    public String payloadMimeType() {
        return "application/avro-binary";
    }
}
