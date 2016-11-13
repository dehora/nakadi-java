package nakadi;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StreamResourceSupportTest {

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:9080").build();

  @Test
  public void testScope() {

    assertEquals(
        TokenProvider.NAKADI_EVENT_STREAM_READ,
        StreamResourceSupport.buildResourceOptions(client,
            new StreamConfiguration().eventTypeName("et")).scope()
    );

    assertEquals(
        TokenProvider.NAKADI_EVENT_STREAM_READ,
        StreamResourceSupport.buildResourceOptions(client,
            new StreamConfiguration().subscriptionId("sub1")).scope()
    );
  }

  @Test
  public void testUrlBuilder() {

    StreamConfiguration et = new StreamConfiguration().eventTypeName("et");
    assertEquals("http://localhost:9080/event-types/et/events",
        StreamResourceSupport.buildStreamUrl(client.baseURI(), et));

    StreamConfiguration s1 = new StreamConfiguration().subscriptionId("s1");
    assertEquals("http://localhost:9080/subscriptions/s1/events",
        StreamResourceSupport.buildStreamUrl(client.baseURI(), s1));

    StreamConfiguration et1 = new StreamConfiguration()
        .eventTypeName("et1")
        .streamKeepAliveLimit(100)
        .streamLimit(101)
        .streamTimeout(102, TimeUnit.SECONDS)
        .batchFlushTimeout(104, TimeUnit.SECONDS)
        .batchLimit(105)
        .maxUncommittedEvents(103) // this should not show up on an event stream uri
        ;

    assertEquals("http://localhost:9080/event-types/et1/events?"
            + "stream_keep_alive_limit=100"
            + "&stream_limit=101"
            + "&batch_limit=105"
            + "&batch_flush_timeout=104"
            + "&stream_timeout=102",
        StreamResourceSupport.buildStreamUrl(client.baseURI(), et1));

    StreamConfiguration s2 = new StreamConfiguration()
        .subscriptionId("s2")
        .streamKeepAliveLimit(100)
        .streamLimit(101)
        .streamTimeout(102, TimeUnit.SECONDS)
        .batchFlushTimeout(104, TimeUnit.SECONDS)
        .batchLimit(105)
        .maxUncommittedEvents(103);

    assertEquals("http://localhost:9080/subscriptions/s2/events?"
            + "stream_keep_alive_limit=100"
            + "&stream_limit=101&batch_limit=105"
            + "&batch_flush_timeout=104"
            + "&max_uncommitted_events=103"
            + "&stream_timeout=102",
        StreamResourceSupport.buildStreamUrl(client.baseURI(), s2));
  }
}