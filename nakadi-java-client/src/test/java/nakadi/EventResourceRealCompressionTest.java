package nakadi;

import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

public class EventResourceRealCompressionTest {

  public static final int MOCK_SERVER_PORT = 8319;
  private final JsonSupport jsonSupport = new GsonSupport();
  private final MockWebServer server = new MockWebServer();

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
  public void sendsCompressedEventWhenAskedTo() throws Exception {

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI("http://localhost:" + MOCK_SERVER_PORT)
        .enablePublishingCompression()
        .build();

    CompressionSupport compressionSupport = client.compressionSupport();

    EventResource resource = client.resources().events();
    EventResourceRealTest.BusinessPayload bp
        = new EventResourceRealTest.BusinessPayload("221", "A1", "B1");
    final EventMetadata metadata = EventMetadata.newPreparedEventMetadata();
    BusinessEventMapped<EventResourceRealTest.BusinessPayload> event =
        new BusinessEventMapped<EventResourceRealTest.BusinessPayload>().metadata(metadata).data(bp);

    try {
      before();

      server.enqueue(new MockResponse().setResponseCode(200));

      resource.send("be-1-200", event);

      RecordedRequest request = server.takeRequest();

      assertTrue("Expecting a content length",
          request.getHeader("Content-Length") != null);
      assertEquals("Expecting to tell the server it's compressed",
          compressionSupport.name(), request.getHeader("Content-Encoding"));
      assertNull("Expecting to not chunk transfer encode the request",
          request.getHeader("Transfer-Encoding"));

      final byte[] zippedRequestEntity = request.getBody().readByteArray();
      final String unzippedRequestEntity = compressionSupport.decompress(zippedRequestEntity);

      Type eventTypeToken = new TypeToken<List<Map<String, Object>>>() {
      }.getType();

      final List<Map<String, Object>> marshaledRequestEntityList
          = jsonSupport.fromJson(unzippedRequestEntity, eventTypeToken);

      final Map<String, Object> marshaledRequestEntity = marshaledRequestEntityList.get(0);

      // check a few of the data fields to see if they round trip back
      assertEquals(event.data().id, marshaledRequestEntity.get("id"));
      assertEquals(event.data().a, marshaledRequestEntity.get("a"));
      assertEquals(event.data().b, marshaledRequestEntity.get("b"));

    } finally {
      after();
    }
  }

  @Test
  public void sendsCompressedRawBatchEventWhenAskedTo() throws Exception {
    NakadiClient client = NakadiClient.newBuilder()
        .baseURI("http://localhost:" + MOCK_SERVER_PORT)
        .enablePublishingCompression()
        .build();

    CompressionSupport compressionSupport = client.compressionSupport();

    EventResource resource = client.resources().events();

    try {
      before();

      String raw = TestSupport.load("data-change-event-single-compress.json");
      server.enqueue(new MockResponse().setResponseCode(200));
      Response r1 = resource.send("ue-1-0", raw);
      assertEquals(200, r1.statusCode());

      RecordedRequest request = server.takeRequest();

      assertNotNull("Expecting a content length", request.getHeader("Content-Length"));
      assertEquals("Expecting to tell the server it's compressed",
          compressionSupport.name(), request.getHeader("Content-Encoding"));
      assertNull("Expecting to not chunk transfer encode the request",
          request.getHeader("Transfer-Encoding"));

      final byte[] zippedRequestEntity = request.getBody().readByteArray();
      final String unzippedRequestEntity = compressionSupport.decompress(zippedRequestEntity);

      Type eventTypeToken = new TypeToken<List<Map<String, Object>>>() {
      }.getType();

      final List<Map<String, Object>> marshaledRequestEntityList
          = jsonSupport.fromJson(unzippedRequestEntity, eventTypeToken);

      final Map<String, Object> marshaledRequestEntity = marshaledRequestEntityList.get(0);

      assertEquals("et-1", marshaledRequestEntity.get("data_type"));
      final Map<?, ?> metadata = (Map<?, ?>) marshaledRequestEntity.get("metadata");
      assertEquals("a2ab0b7c-ee58-48e5-b96a-d13bce73d857", metadata.get("eid"));
      assertEquals("Nt0oU70k3UCNp2NKugrIF0QU", metadata.get("flow_id"));

    } finally {
      after();
    }
  }

}
