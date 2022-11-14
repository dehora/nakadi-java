package nakadi;

import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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


  public static class Model {
    Integer id;
    Double num;
  }

  @Test
  public void intsAndDoublesAreNotCoerced_gh119() throws Exception {

    final Model m = new Model();
    m.id = 100;
    m.num = 26.3;

    BusinessEventMapped<Model> change = new BusinessEventMapped<Model>()
        .metadata(new EventMetadata())
        .data(m);

    server.enqueue(new MockResponse().setResponseCode(200));

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI("http://localhost:"+8311)
        .build();

    client.resources().events().send("et", change);

    final RecordedRequest request = server.takeRequest();
    final String s = request.getBody().readUtf8();
    assertFalse("expecting ints to not be converted to floats by gson", s.contains(":100.0"));
    assertTrue("expecting ints to be present if set", s.contains(":100"));
    assertTrue("expecting floats to be present if set", s.contains(":26.3"));
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

          buildResource().requestThrowing("POST", baseUrl(), buildOptionsWithJsonContent(), () -> json.toJsonBytes("{}"));
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

        buildResource().requestThrowing("POST", baseUrl(), buildOptionsWithJsonContent(), () -> json.toJsonBytes("{}"), String.class);
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
        buildOptionsWithJsonContent(),
        () -> json.toJsonBytes(request),
        Subscription.class);

    assertEquals("fake-uuid", response.id());
  }

  private ResourceOptions buildOptions() {
    return ResourceSupport.options("application/json").tokenProvider(scope -> Optional.empty());
  }

  private ResourceOptions buildOptionsWithJsonContent() {
    return ResourceSupport.
            optionsWithJsonContent(ResourceSupport.options("application/json").tokenProvider(scope -> Optional.empty()));
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
            .contains("no_retry_cowardly");
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
            .contains("no_retry_cowardly");
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
  }

  @Test
  public void requestThrowingWithBody() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(200));

    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptionsWithJsonContent();

    Subscription subscription = buildSubscription();

    r.requestThrowing("POST", "http://localhost:"+ MOCK_SERVER_PORT +"/subscriptions", options, () -> json.toJsonBytes(subscription));
    RecordedRequest request = server.takeRequest();

    assertEquals("POST /subscriptions HTTP/1.1", request.getRequestLine());
    assertEquals(ResourceSupport.APPLICATION_JSON_CHARSET_UTF_8, request.getHeaders().get("Content-Type"));
    assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));
    assertTrue(request.getHeaders().get("X-Flow-Id") != null);

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
  public void listWithRetry() {
    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:"+9881)
            .build());

    OkHttpResource r0 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));


    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r0);

    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .initialInterval(2, TimeUnit.SECONDS)
        .maxAttempts(3)
        .maxInterval(2, TimeUnit.SECONDS)
        .build();

    try {
      final SubscriptionCollection list = new SubscriptionResourceReal(client)
          .retryPolicy(exponentialRetry)
          .list();
    } catch (RetryableException | NotFoundException ignored) {
      ignored.printStackTrace();
    }

    verify(r0, times(3)).okHttpRequest(Matchers.anyObject());
  }

  @Test
  public void checkpointWithNoScope() {

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {

              if (scope != null) {
                throw new AssertionError("scope should not be called");
              }

              return Optional.empty();
            })
            .build());

    Resource r0 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r0);

    Cursor c = new Cursor().cursorToken("ctoken").eventType("et1").offset("0").partition("1");

    Map<String, Object> requestMap = Maps.newHashMap();
    requestMap.put("items", Lists.newArrayList(c));

    HashMap<String, String> context = Maps.newHashMap();
    context.put("X-Nakadi-StreamId", "aa");
    context.put("SubscriptionId", "woo");

    try {

      RetryPolicy backoff = ExponentialRetry.newBuilder()
          .initialInterval(1, TimeUnit.SECONDS)
          .maxAttempts(1)
          .maxInterval(1, TimeUnit.SECONDS)
          .build();

      new SubscriptionResourceReal(client)
          .checkpoint(backoff, context, c);
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9081/subscriptions/woo/cursors"),
        options.capture(),
        Matchers.any(ContentSupplier.class)
    );


    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {

      RetryPolicy backoff = ExponentialRetry.newBuilder()
          .initialInterval(1, TimeUnit.SECONDS)
          .maxAttempts(1)
          .maxInterval(1, TimeUnit.SECONDS)
          .build();

      SubscriptionResource resource = new SubscriptionResourceReal(client);

      // call the inner method to control the backoff, the interface method's just a wrapper
      ((SubscriptionResourceReal)resource).checkpoint(backoff, context, c);

    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9081/subscriptions/woo/cursors"),
        options.capture(),
        Matchers.any(ContentSupplier.class)
    );

    assertEquals(null, options.getValue().scope());
  }

  @Test
  public void requestThrowingBody() throws Exception {

    OkHttpResource r = buildResource();
    ResourceOptions options = buildOptionsWithJsonContent();
    server.enqueue(new MockResponse().setResponseCode(200));

    Subscription subscription = buildSubscription();

    r.requestThrowing("POST", "http://localhost:" + MOCK_SERVER_PORT + "/subscriptions", options,
        () -> json.toJsonBytes(subscription));
    RecordedRequest request = server.takeRequest();

    assertEquals("POST /subscriptions HTTP/1.1", request.getRequestLine());
    assertEquals(ResourceSupport.APPLICATION_JSON_CHARSET_UTF_8, request.getHeaders().get("Content-Type"));
    assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));
    assertTrue(request.getHeaders().get("X-Flow-Id") != null);

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
