package nakadi;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class StreamResourceSupportTest {

  private static final int MOCK_SERVER_PORT = 8314;

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI(baseUrl()).build();

  @Test
  public void testNoScope() {

    assertNull(
        StreamResourceSupport.buildResourceOptions(client,
            new StreamConfiguration().eventTypeName("et")).scope()
    );

    assertNull(
        StreamResourceSupport.buildResourceOptions(client,
            new StreamConfiguration().subscriptionId("sub1")).scope()
    );

    assertNull(
        StreamResourceSupport.buildResourceOptions(client,
            new StreamConfiguration().eventTypeName("et")).scope()
    );

    assertNull(
        StreamResourceSupport.buildResourceOptions(client,
            new StreamConfiguration().subscriptionId("sub1")).scope()
    );
  }

  @Test
  public void testUrlBuilder() {

    StreamConfiguration et = new StreamConfiguration().eventTypeName("et");
    assertEquals(baseUrl()+ "/event-types/et/events",
        StreamResourceSupport.buildStreamUrl(client.baseURI(), et));

    StreamConfiguration s1 = new StreamConfiguration().subscriptionId("s1");
    assertEquals(baseUrl()+ "/subscriptions/s1/events",
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

    assertEquals(baseUrl() + "/event-types/et1/events?"
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

    assertEquals(baseUrl()+ "/subscriptions/s2/events?"
            + "stream_keep_alive_limit=100"
            + "&stream_limit=101&batch_limit=105"
            + "&batch_flush_timeout=104"
            + "&max_uncommitted_events=103"
            + "&stream_timeout=102",
        StreamResourceSupport.buildStreamUrl(client.baseURI(), s2));
  }

  @Test
  public void testUrlBuilderBatchLimit_gh125() {
    // see https://github.com/zalando-incubator/nakadi-java/issues/125

    StreamConfiguration et1 = new StreamConfiguration()
        .eventTypeName("et1")
        .batchLimit(0);

    assertEquals(baseUrl()+ "/event-types/et1/events",
        StreamResourceSupport.buildStreamUrl(client.baseURI(), et1));
  }

  @Test
  public void testHeaderConfiguration() {

    StreamConfiguration sc = new StreamConfiguration();
    assertNotNull(sc.requestHeaders());

    try {
      new StreamConfiguration().requestHeaders(null);
      fail("should not accept null headers");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Please provide non null request headers"));
    }

    Map<String, String> headers = Maps.newHashMap();
    headers.put("Accept-Encoding", "gzip");
    sc = new StreamConfiguration().requestHeaders(headers);

    assertNotNull(sc.requestHeaders());
    assertSame(1, sc.requestHeaders().size());
    assertTrue(sc.requestHeaders().containsKey("Accept-Encoding"));

    sc.requestHeader("User-Agent", "acme-client");
    assertSame(2, sc.requestHeaders().size());
    assertTrue(sc.requestHeaders().containsKey("Accept-Encoding"));
    assertTrue(sc.requestHeaders().containsKey("User-Agent"));
  }

  @Test
  public void testHeaderConfigurationIsApplied() {
    StreamConfiguration sc = new StreamConfiguration();
    sc.requestHeader("Accept-Encoding", "gzip");

    NakadiClient client =
        NakadiClient.newBuilder().baseURI(baseUrl()).build();

    final ResourceOptions resourceOptions =
        StreamResourceSupport.buildResourceOptions(client, sc);

    assertTrue(resourceOptions.headers().containsKey("Accept-Encoding"));
  }

  private String baseUrl() {
    return "http://localhost:" + MOCK_SERVER_PORT;
  }
}
