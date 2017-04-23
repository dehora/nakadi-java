package nakadi;

import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
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
import org.mockito.ArgumentMatcher;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OkHttpResourceTest {

  public static final int MOCK_SERVER_PORT = 8311;
  private final MetricCollectorDevnull collector = new MetricCollectorDevnull();
  private GsonSupport json = new GsonSupport();
  MockWebServer server = new MockWebServer();

  @Before
  public void before() throws Exception {
    server.start(InetAddress.getByName("localhost"), MOCK_SERVER_PORT);
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

        buildResource().requestThrowing("GET", baseUrl(), buildOptions());
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

        buildResource().requestThrowing("POST", baseUrl(), buildOptions(), "{}");
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

        buildResource().requestThrowing("POST", baseUrl(), buildOptions(), String.class);
        fail("expected exception for " + entry.getValue());
      } catch (NakadiException e) {
        assertEquals(entry.getValue(), e.getClass());
        assertEquals(Problem.T1000_TYPE, e.problem().type());
      }
    }
  }

  @Test
  public void requestThrowing_WithBody_WithBinding_ExceptionMapping() throws Exception {

    for (Map.Entry<Integer, Class> entry : responseCodesToExceptions().entrySet()) {
      try {
        server.enqueue(new MockResponse().setResponseCode(entry.getKey()));

        buildResource().requestThrowing("POST", baseUrl(), buildOptions(), "{}", String.class);
        fail("expected exception for " + entry.getValue());
      } catch (NakadiException e) {
        assertEquals(entry.getValue(), e.getClass());
        assertEquals(Problem.T1000_TYPE, e.problem().type());
      }
    }
  }

  @Test
  public void request_WithBody_WithBinding() throws Exception {

    Subscription request = buildSubscription().id("fake-uuid");

    server.enqueue(new MockResponse().setResponseCode(200).setBody(json.toJson(request)));

    Subscription response = buildResource().requestThrowing(
        "POST",
        baseUrl(),
        buildOptions(),
        request,
        Subscription.class);

    assertEquals("fake-uuid", response.id());
  }

  private ResourceOptions buildOptions() {
    return ResourceSupport.options("application/json").tokenProvider(scope -> Optional.empty());
  }

  private Map<Integer, Class> responseCodesToExceptions() {
    return ExceptionSupport.responseCodesToExceptionsMap();
  }

  @Test
  public void requestRetryingSkipThrowing() throws Exception {

    final int[] retrySkipCount = {0};

    OkHttpResource r = new OkHttpResource(new OkHttpClient.Builder().build(), json,
        new MetricCollector() {
          @Override public void mark(Meter meter) {
            if(meter.equals(Meter.retrySkipFinished)) {
              retrySkipCount[0]++;
            }
          }

          @Override public void mark(Meter meter, long count) {

          }

          @Override public void duration(Timer timer, long duration, TimeUnit unit) {

          }
        });
    ResourceOptions options = buildOptions();

    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(200));

    ExponentialRetry retry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.SECONDS)
        .maxAttempts(3)
        .maxInterval(3, TimeUnit.SECONDS)
        .build();


      assertEquals(200,
          r.retryPolicy(retry).request("GET", baseUrl(), options).statusCode());


    // now check we don't retry with an exhausted policy
    assertTrue(retry.isFinished());
    assertTrue(retrySkipCount[0] == 0);

    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(200));

    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
        ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    final Appender mockAppender = mock(Appender.class);
    when(mockAppender.getName()).thenReturn("mockito");
    root.addAppender(mockAppender);

    try {
      /*
      we're calling a dud retry against requestThrowing; this should throw, not return a response
       */
      r.retryPolicy(retry)
          .requestThrowing("GET", baseUrl(), options);
      fail();
    } catch (RateLimitException ignored) {
    }

    assertTrue(retrySkipCount[0] == 1);

    verify(mockAppender).doAppend(argThat(new ArgumentMatcher() {
      @Override
      public boolean matches(final Object argument) {
        return ((LoggingEvent) argument).getFormattedMessage()
            .contains("Cowardly, refusing to apply retry policy that is already finished");
      }
    }));
  }


  @Test
  public void requestRetryingSkipNotThrowing() throws Exception {

    final int[] retrySkipCount = {0};

    OkHttpResource r = new OkHttpResource(new OkHttpClient.Builder().build(), json,
        new MetricCollector() {
          @Override public void mark(Meter meter) {
            if(meter.equals(Meter.retrySkipFinished)) {
              retrySkipCount[0]++;
            }
          }

          @Override public void mark(Meter meter, long count) {

          }

          @Override public void duration(Timer timer, long duration, TimeUnit unit) {

          }
        });
    ResourceOptions options = buildOptions();

    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(200));

    ExponentialRetry retry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.SECONDS)
        .maxAttempts(3)
        .maxInterval(3, TimeUnit.SECONDS)
        .build();


    assertEquals(200,
        r.retryPolicy(retry).request("GET", baseUrl(), options).statusCode());


    // now check we don't retry with an exhausted policy
    assertTrue(retry.isFinished());
    assertTrue(retrySkipCount[0] == 0);

    server.enqueue(new MockResponse().setResponseCode(429));
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(200));

    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
        ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    final Appender mockAppender = mock(Appender.class);
    when(mockAppender.getName()).thenReturn("mockito");
    root.addAppender(mockAppender);

    try {
      /*
      we're calling a dud retry against request; this should return a response and not throw
       */
      Response response = r.retryPolicy(retry)
          .request("GET", baseUrl(), options);

      assertEquals(429, response.statusCode());

    } catch (RateLimitException ignored) {
      fail();
    }

    assertTrue(retrySkipCount[0] == 1);

    verify(mockAppender).doAppend(argThat(new ArgumentMatcher() {
      @Override
      public boolean matches(final Object argument) {
        return ((LoggingEvent) argument).getFormattedMessage()
            .contains("Cowardly, refusing to apply retry policy that is already finished");
      }
    }));
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
      r.requestThrowing("GET", baseUrl(), options);
    } catch (RateLimitException e) {
      // also test we got no problem back from the server and handled it
      assertEquals(Problem.T1000_TYPE, e.problem().type());
    }

    // retry past the remaining 2 429s

    ExponentialRetry retry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.SECONDS)
        .maxAttempts(3)
        .maxInterval(3, TimeUnit.SECONDS)
        .build();

    /*
    note that we're testing retrys with a non-throwing request option; internally we'll
    throw exceptions to drive the rx retry mechanism and if that doesn't work out we'll
     return the last response from the server instead of throwing the exception. This
     is for coverage only; the sanest way to use retries is with requestThrowing
     */
      assertEquals(200,
          r.retryPolicy(retry).request("GET", baseUrl(), options).statusCode());

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

    r.request("POST", "http://localhost:"+ MOCK_SERVER_PORT +"/subscriptions", options, subscription);
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
      r.requestThrowing("GET", baseUrl(), options);
    } catch (RateLimitException e) {
      // also test we got no problem back from the server and handled it
      assertEquals(Problem.T1000_TYPE, e.problem().type());
    }

    ExponentialRetry retry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.SECONDS)
        .maxAttempts(3)
        .maxInterval(3, TimeUnit.SECONDS)
        .build();

    assertEquals(200,
        r.retryPolicy(retry)
            .requestThrowing("GET", baseUrl(), options)
            .statusCode());
  }

  @Test
  public void requestThrowingBody() throws Exception {

    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptions();
    server.enqueue(new MockResponse().setResponseCode(200));

    Subscription subscription = buildSubscription();

    r.requestThrowing("POST", "http://localhost:"+ MOCK_SERVER_PORT +"/subscriptions", options, subscription);
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
        r.requestThrowing("GET", baseUrl() + "/subscriptions/thingies", options,
            Subscription.class);

    assertEquals(subscription, get);

  }

  private String baseUrl() {
    return "http://localhost:" + MOCK_SERVER_PORT;
  }

  private Subscription buildSubscription() {
    return new Subscription()
        .consumerGroup("mccaffrey-cg")
        .eventType("ue-1-1479125860")
        .owningApplication("shaper");
  }
}