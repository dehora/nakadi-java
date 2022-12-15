package nakadi.avro;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import nakadi.BusinessEventMapped;
import nakadi.EventMetadata;
import nakadi.SerializationContext;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.generated.avro.PublishingBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

public class AvroPublishingBatchSerializerTest {

    private final String schema = "{\"type\":\"record\",\"name\":\"BusinessPayload\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"string\"]},{\"name\":\"b\",\"type\":[\"null\",\"string\"]},{\"name\":\"id\",\"type\":[\"null\",\"string\"]}]}";

    @Test
    public void testToBytes() throws IOException {
        BusinessPayload bp = new BusinessPayload("22", "A", "B");
        BusinessEventMapped<BusinessPayload> event =
                new BusinessEventMapped<BusinessPayload>()
                        .metadata(EventMetadata.newPreparedEventMetadata())
                        .data(bp);

        AvroPublishingBatchSerializer avroPublishingBatchSerializer = new AvroPublishingBatchSerializer(new AvroMapper());
        byte[] bytesBatch = avroPublishingBatchSerializer.toBytes(
                new TestSerializationContext("ad-2022-12-13", schema, "1.0.0"),
                Collections.singletonList(event)
        );

        PublishingBatch publishingBatch = PublishingBatch.fromByteBuffer(ByteBuffer.wrap(bytesBatch));
        byte[] eventBytes = publishingBatch.getEvents().get(0).getPayload().array();
        BusinessPayload actual = new AvroMapper().reader(
                new AvroSchema(new Schema.Parser().parse(schema)))
                .readValue(eventBytes, BusinessPayload.class);

        Assert.assertEquals(bp, actual);
    }

    @Test
    public void testToBytes() throws IOException {
        BusinessPayload bp = new BusinessPayload("22", "A", "B");
        BusinessEventMapped<BusinessPayload> event =
                new BusinessEventMapped<BusinessPayload>()
                        .metadata(EventMetadata.newPreparedEventMetadata())
                        .data(bp);

        AvroPublishingBatchSerializer avroPublishingBatchSerializer = new AvroPublishingBatchSerializer(new AvroMapper());
        byte[] bytesBatch = avroPublishingBatchSerializer.toBytes(
                new TestSerializationContext("ad-2022-12-13", schema, "1.0.0"),
                Collections.singletonList(event)
        );

        PublishingBatch publishingBatch = PublishingBatch.fromByteBuffer(ByteBuffer.wrap(bytesBatch));
        byte[] eventBytes = publishingBatch.getEvents().get(0).getPayload().array();
        BusinessPayload actual = new AvroMapper().reader(
                new AvroSchema(new Schema.Parser().parse(schema)))
                .readValue(eventBytes, BusinessPayload.class);

        Assert.assertEquals(bp, actual);
    }


    static class BusinessPayload {
        String id;
        String a;
        String b;

        public BusinessPayload() {
        }

        public BusinessPayload(String id, String a, String b) {
            this.id = id;
            this.a = a;
            this.b = b;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }

        public String getB() {
            return b;
        }

        public void setB(String b) {
            this.b = b;
        }

        @Override
        public String toString() {
            return "BusinessPayload{" + "id='" + id + '\'' +
                    ", a='" + a + '\'' +
                    ", b='" + b + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BusinessPayload that = (BusinessPayload) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(a, that.a) &&
                    Objects.equals(b, that.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, a, b);
        }
    }

    private class TestSerializationContext implements SerializationContext {

        private String name;
        private String schema;
        private String version;

        public TestSerializationContext(String name, String schema, String version) {
            this.name = name;
            this.schema = schema;
            this.version = version;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String schema() {
            return schema;
        }

        @Override
        public String version() {
            return version;
        }
    }
}
