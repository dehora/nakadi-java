package nakadi;

import java.net.InetAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class OkHttpResourceTest {

  private final MetricCollectorDevnull collector = new MetricCollectorDevnull();
  private GsonSupport json = new GsonSupport();
  MockWebServer server = new MockWebServer();

  @Before
  public void before() throws Exception {
    server.start(InetAddress.getByName("localhost"), 8311);
  }

  @After
  public void after() throws Exception {
    server.shutdown();
  }

  @Test
  public void requestThrowing_NoBody_ExceptionMapping() throws Exception {

    for (Map.Entry<Integer, Class> entry : responseCodesToExceptions().entrySet()) {
      try {
        server.enqueue(new MockResponse().setResponseCode(entry.getKey()));

        buildResource().requestThrowing("GET", "http://localhost:8311/", buildOptions());
        fail("expected exception for " + entry.getValue());

      } catch (NakadiException e) {
        assertEquals(entry.getValue(), e.getClass());
        // also test we got no problem back from the server and could handle it
        assertEquals(Problem.T1000_TYPE, e.problem().type());
      }
    }
  }

  private OkHttpResource buildResource() {
    return new OkHttpResource(new OkHttpClient.Builder().build(), json, collector);
  }

  @Test
  public void requestThrowing_WithBody_ExceptionMapping() throws Exception {

    for (Map.Entry<Integer, Class> entry : responseCodesToExceptions().entrySet()) {
      try {
        server.enqueue(new MockResponse().setResponseCode(entry.getKey()));

        buildResource().requestThrowing("POST", "http://localhost:8311/", buildOptions(), "{}");
        fail("expected exception for " + entry.getValue());

      } catch (NakadiException e) {
        assertEquals(entry.getValue(), e.getClass());
        assertEquals(Problem.T1000_TYPE, e.problem().type());
      }
    }
  }

  @Test
  public void requestThrowing_WithBinding_ExceptionMapping() throws Exception {

    for (Map.Entry<Integer, Class> entry : responseCodesToExceptions().entrySet()) {
      try {
        server.enqueue(new MockResponse().setResponseCode(entry.getKey()));

        buildResource().requestThrowing("POST", "http://localhost:8311/", buildOptions(), String.class);
        fail("expected exception for " + entry.getValue());
      } catch (NakadiException e) {
        assertEquals(entry.getValue(), e.getClass());
        assertEquals(Problem.T1000_TYPE, e.problem().type());
      }
    }
  }

  private ResourceOptions buildOptions() {
    return ResourceSupport.options("application/json").tokenProvider(scope -> Optional.empty());
  }

  private Map<Integer, Class> responseCodesToExceptions() {
    return ExceptionSupport.responseCodesToExceptionsMap();
  }

  @Test
  public void requestRetrying() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(200));

    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptions();

    try {
      r.request("GET", "http://localhost:8311/", options);
    } catch (RateLimitException e) {
      // also test we got no problem back from the server and handled it
      assertEquals(Problem.T1000_TYPE, e.problem().type());
    }

    // retry past the remaining 2 429s

    ExponentialRetry retry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.MILLISECONDS)
        .maxAttempts(3)
        .maxInterval(3, TimeUnit.MILLISECONDS)
        .build();

    assertEquals(200,
        r.retryPolicy(retry).request("GET", "http://localhost:8311/", options).statusCode());

    RecordedRequest request = server.takeRequest();

    assertEquals("GET / HTTP/1.1", request.getRequestLine());
    assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));
    assertTrue(request.getHeaders().get("X-Flow-Id") != null);
    assertTrue(request.getHeaders().get("X-Client-Platform-Details") != null);
  }

  @Test
  public void requestWithBody() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(200));

    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptions();

    Subscription subscription = buildSubscription();

    r.request("POST", "http://localhost:8311/subscriptions", options, subscription);
    RecordedRequest request = server.takeRequest();

    assertEquals("POST /subscriptions HTTP/1.1", request.getRequestLine());
    assertEquals("application/json; charset=utf8", request.getHeaders().get("Content-Type"));
    assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));
    assertTrue(request.getHeaders().get("X-Flow-Id") != null);
    assertTrue(request.getHeaders().get("X-Client-Platform-Details") != null);

    Subscription fromJson = json.fromJson(request.getBody().readUtf8(), Subscription.class);
    assertEquals(subscription, fromJson);
  }

  @Test
  public void requestThrowingRetries() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(200));

    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptions();

    try {
      r.requestThrowing("GET", "http://localhost:8311/", options);
    } catch (RateLimitException e) {
      // also test we got no problem back from the server and handled it
      assertEquals(Problem.T1000_TYPE, e.problem().type());
    }

    ExponentialRetry retry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.MILLISECONDS)
        .maxAttempts(3)
        .maxInterval(3, TimeUnit.MILLISECONDS)
        .build();

    assertEquals(200,
        r.retryPolicy(retry)
            .requestThrowing("GET", "http://localhost:8311/", options)
            .statusCode());
  }

  @Test
  public void requestThrowingBody() throws Exception {

    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptions();
    server.enqueue(new MockResponse().setResponseCode(200));

    Subscription subscription = buildSubscription();

    r.requestThrowing("POST", "http://localhost:8311/subscriptions", options, subscription);
    RecordedRequest request = server.takeRequest();

    assertEquals("POST /subscriptions HTTP/1.1", request.getRequestLine());
    assertEquals("application/json; charset=utf8", request.getHeaders().get("Content-Type"));
    assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));
    assertTrue(request.getHeaders().get("X-Flow-Id") != null);
    assertTrue(request.getHeaders().get("X-Client-Platform-Details") != null);

    Subscription fromJson = json.fromJson(request.getBody().readUtf8(), Subscription.class);
    assertEquals(subscription, fromJson);
  }

  @Test
  public void requestThrowingResponse() throws Exception {
    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptions();

    Subscription subscription = buildSubscription();

    String raw = json.toJson(subscription);

    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(raw)
        .setHeader("Content-Type", "application/json"));

    Subscription get =
        r.requestThrowing("GET", "http://localhost:8311/subscriptions/thingies", options,
            Subscription.class);

    assertEquals(subscription, get);

  }

  private Subscription buildSubscription() {
    return new Subscription()
          .consumerGroup("mccaffrey-cg")
          .eventType("ue-1-1479125860")
          .owningApplication("shaper");
  }
}