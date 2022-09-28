package nakadi;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import org.zalando.nakadi.generated.avro.PublishingBatch;
import org.zalando.nakadi.generated.avro.test.SomeEvent;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

public class EventResourceRealBinaryTest extends EventResourceRealTest {

    @Test
    public void sendBinaryEvent() throws Exception {
        final NakadiClient client = spy(NakadiClient.newBuilder()
                .baseURI("http://localhost:" + MOCK_SERVER_PORT)
                .build());

        final EventResource resource = client.resources().eventsBinary(
                "2C0A51FD-32ED-4FF4-8528-283F6B4C35EF",
                "1.0.0",
                SomeEvent.getClassSchema()
        );

        final SomeEvent someEvent = SomeEvent.newBuilder()
                .setFoo(100)
                .setFoo2("bar")
                .build();

        final EventEnvelope<SomeEvent> envelope = new EventEnvelope(someEvent,
                EventMetadata.newPreparedEventMetadata()
                        .eventType("2C0A51FD-32ED-4FF4-8528-283F6B4C35EF")
                        .version("1.0.0"));

        try {
            before();

            server.enqueue(new MockResponse().setResponseCode(200));
            resource.send("2C0A51FD-32ED-4FF4-8528-283F6B4C35EF", envelope);
            final RecordedRequest request = server.takeRequest();
            assertEquals(
                    "POST /event-types/2C0A51FD-32ED-4FF4-8528-283F6B4C35EF/events HTTP/1.1",
                    request.getRequestLine());
            assertEquals("application/avro-binary", request.getHeaders().get("Content-Type"));

            final PublishingBatch actualBatch = PublishingBatch.fromByteBuffer(
                    ByteBuffer.wrap(request.getBody().readByteArray()));

            final SomeEvent actualEvent = new SpecificDatumReader<SomeEvent>(SomeEvent.getClassSchema())
                    .read(null, DecoderFactory.get().binaryDecoder(
                            actualBatch.getEvents().get(0).getPayload().array(), null));
            assertEquals(someEvent, actualEvent);
        } finally {
            after();
        }
    }
}
