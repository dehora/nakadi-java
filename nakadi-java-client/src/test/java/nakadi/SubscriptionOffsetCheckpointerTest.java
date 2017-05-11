package nakadi;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SubscriptionOffsetCheckpointerTest {

  private static final int MOCK_SERVER_PORT = 8315;

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:" + MOCK_SERVER_PORT).build();

  private MockWebServer server = new MockWebServer();

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
  public void suppressesNetwork() throws InvalidException {

    Cursor cursor = new Cursor("p", "o", "e");
    final HashMap<String, String> context = Maps.newHashMap();
    context.put(StreamResourceSupport.X_NAKADI_STREAM_ID, "aa");
    context.put(StreamResourceSupport.SUBSCRIPTION_ID, "bb");
    StreamCursorContext streamCursorContext = new StreamCursorContextReal(cursor, context);

    SubscriptionOffsetCheckpointer checkpointer = new SubscriptionOffsetCheckpointer(client);
    checkpointer = spy(checkpointer);
    doThrow(NetworkException.class).when(checkpointer).checkpointInner(any(), any());

    try {
      checkpointer.checkpoint(streamCursorContext);
      fail("expecting a network error");
    } catch (NetworkException ignored) {
    }

    checkpointer = new SubscriptionOffsetCheckpointer(client).suppressNetworkException(true);
    checkpointer = spy(checkpointer);
    doThrow(new NetworkException(Problem.networkProblem("fake", "it")))
        .when(checkpointer)
        .checkpointInner(any(), any());

    try {
      checkpointer.checkpoint(streamCursorContext);
    } catch (NetworkException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void suppressesInvalid() throws InvalidException {

    String errJson = TestSupport.load("err_422_session_id.json");

    try {
      before();

      SubscriptionOffsetCheckpointer checkpointer = new SubscriptionOffsetCheckpointer(client);

      Cursor cursor = new Cursor("p", "o", "e");
      final HashMap<String, String> context = Maps.newHashMap();
      context.put(StreamResourceSupport.X_NAKADI_STREAM_ID, "aa");
      context.put(StreamResourceSupport.SUBSCRIPTION_ID, "bb");

      StreamCursorContext streamCursorContext = new StreamCursorContextReal(cursor, context);

      try {
        server.enqueue(new MockResponse().setResponseCode(422).setBody(errJson));
        checkpointer.checkpoint(streamCursorContext);
        fail("expecting a 422 error");
      } catch (InvalidException e) {
        assertEquals(422, e.problem().status());
      }

      try {
        server.enqueue(new MockResponse().setResponseCode(422).setBody(errJson));
        checkpointer.checkpoint(streamCursorContext, true);
      } catch (Exception e) {
        fail(e.getMessage());
      }

      checkpointer = new SubscriptionOffsetCheckpointer(client, true);
      try {
        server.enqueue(new MockResponse().setResponseCode(422).setBody(errJson));
        checkpointer.checkpoint(streamCursorContext, true);
      } catch (Exception e) {
        fail("expected default true to suppress errors " + e.getMessage());
      }

      try {
        server.enqueue(new MockResponse().setResponseCode(422).setBody(errJson));
        checkpointer.checkpoint(streamCursorContext, false);
        fail("expecting overriding default true to produce a 422 error");
      } catch (InvalidException e) {
        assertEquals(422, e.problem().status());
      }


    } finally {
      after();
    }
  }
}