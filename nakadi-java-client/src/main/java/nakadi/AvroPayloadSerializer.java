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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroPayloadSerializer implements PayloadSerializer {

    private Map<String, EventTypeSchemaPair<Schema>> etSchemas;
    public AvroPayloadSerializer(Map<String, EventTypeSchemaPair<Schema>> etSchemas) {
        this.etSchemas = etSchemas;
    }

    @Override
    public <T> byte[] toBytes(final String eventTypeName, final Collection<T> events) {
        try {
            final List<Envelope> envelops = events.stream()
                    .map(event -> {
                        EventEnvelope realEvent = (EventEnvelope) event;
                        try {
                            final ByteArrayOutputStream payloadOutStream = new ByteArrayOutputStream();
                            EventTypeSchemaPair<Schema> etSchemaPair = etSchemas.get(realEvent.getMetadata().eventType());
                            if(etSchemaPair == null){
                                throw new InvalidEventTypeException("Unexpected event-type "+ realEvent.getMetadata().eventType() + " provided during avro serialization");
                            }
                            new GenericDatumWriter(etSchemaPair.schema()).write(realEvent.data(),
                                    EncoderFactory.get().directBinaryEncoder(payloadOutStream, null));

                            final EventMetadata metadata = realEvent.getMetadata();
                            return Envelope.newBuilder()
                                    .setMetadata(Metadata.newBuilder()
                                            .setEventType(metadata.eventType())
                                            .setVersion(etSchemaPair.version())
                                            .setOccurredAt(metadata.occurredAt().toInstant())
                                            .setEid(metadata.eid())
                                            .setPartition(metadata.partition())
                                            .setPartitionCompactionKey(metadata.partitionCompactionKey())
                                            .build())
                                    .setPayload(ByteBuffer.wrap(payloadOutStream.toByteArray()))
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
    public String payloadMimeType() {
        return "application/avro-binary";
    }
}
