package nakadi;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import org.zalando.nakadi.generated.avro.PublishingBatch;
import org.zalando.nakadi.generated.avro.test.SomeEvent;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.spy;

public class EventResourceRealBinaryTest {

    public static final int MOCK_SERVER_PORT = 8317;

    MockWebServer server = new MockWebServer();
    GsonSupport json = new GsonSupport();

    public void before() {
        try {
            server.start(InetAddress.getByName("localhost"), MOCK_SERVER_PORT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void after() {
        try {
            server.shutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void exceptionOnMismatchSchema(){
        final NakadiClient client = spy(NakadiClient.newBuilder()
                .baseURI("http://localhost:" + MOCK_SERVER_PORT)
                .build());
        try {
            before();
            server.enqueue(new MockResponse().setResponseCode(404));
            Exception ex = assertThrows(InvalidSchemaException.class, () ->
                    client.resources().eventsBinary(
                            new EventTypeSchemaPair<Schema>().eventTypeName("2C0A51FD-32ED-4FF4-8528-283F6B4C35EF").version("2.0.0").schema(SomeEvent.getClassSchema()))) ;

            assertEquals("No matching schemas found for event types [2C0A51FD-32ED-4FF4-8528-283F6B4C35EF]", ex.getMessage());
        }finally {
            after();
        }
    }

    @Test
    public void sendBinaryEvent() throws Exception {
        final NakadiClient client = spy(NakadiClient.newBuilder()
                .baseURI("http://localhost:" + MOCK_SERVER_PORT)
                .build());


        final SomeEvent someEvent = SomeEvent.newBuilder()
                .setFoo(100)
                .setFoo2("bar")
                .build();

        final EventEnvelope<SomeEvent> envelope = new EventEnvelope(someEvent,
                EventMetadata.newPreparedEventMetadata()
                        .eventType("2C0A51FD-32ED-4FF4-8528-283F6B4C35EF")
                        .version("1.0.0"));
        EventTypeSchema eventTypeSchemaResponse = new EventTypeSchema().
                schema(SomeEvent.getClassSchema().toString()).
                version("1.0.0").
                type(EventTypeSchema.Type.avro_schema);

        try {
            before();
            server.enqueue(new MockResponse().setResponseCode(200).setBody(json.toJson(eventTypeSchemaResponse)));
            final EventResource resource = client.resources().eventsBinary(
                    new EventTypeSchemaPair<Schema>().eventTypeName("2C0A51FD-32ED-4FF4-8528-283F6B4C35EF").
                            version("1.0.0").
                            schema(SomeEvent.getClassSchema())
            );
            server.takeRequest();
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

    @Test
    public void failSendOnIncorrectEventTypeInMetadata() throws Exception {
        final NakadiClient client = spy(NakadiClient.newBuilder()
                .baseURI("http://localhost:" + MOCK_SERVER_PORT)
                .build());


        final SomeEvent someEvent = SomeEvent.newBuilder()
                .setFoo(100)
                .setFoo2("bar")
                .build();

        final EventEnvelope<SomeEvent> envelope = new EventEnvelope(someEvent,
                EventMetadata.newPreparedEventMetadata()
                        .eventType("dummy-event-type")
                        .version("1.0.0"));
        EventTypeSchema eventTypeSchemaResponse = new EventTypeSchema().
                schema(SomeEvent.getClassSchema().toString()).
                version("1.0.0").
                type(EventTypeSchema.Type.avro_schema);

        try {
            before();
            server.enqueue(new MockResponse().setResponseCode(200).setBody(json.toJson(eventTypeSchemaResponse)));
            final EventResource resource = client.resources().eventsBinary(
                    new EventTypeSchemaPair<Schema>().eventTypeName("2C0A51FD-32ED-4FF4-8528-283F6B4C35EF").
                            version("1.0.0").
                            schema(SomeEvent.getClassSchema())
            );
            server.takeRequest();
            server.enqueue(new MockResponse().setResponseCode(200));

            Exception ex = assertThrows(InvalidEventTypeException.class, () ->
                    resource.send("2C0A51FD-32ED-4FF4-8528-283F6B4C35EF", envelope)
            );
            assertEquals("Unexpected event-type dummy-event-type provided during avro serialization", ex.getMessage());

        } finally {
            after();
        }
    }
}
