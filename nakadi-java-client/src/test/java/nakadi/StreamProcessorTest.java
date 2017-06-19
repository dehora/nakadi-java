package nakadi;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Callable;
import junit.framework.TestCase;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class StreamProcessorTest {

  private static final int MOCK_SERVER_PORT = 8313;

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:9081").build();
  private StreamProcessor processor;

  private MockWebServer server = new MockWebServer();
  private final String batch = TestSupport.load("data-change-event-batch-oneline-1.json");

  @Before
  public void before() throws Exception {
    server.start(InetAddress.getByName("localhost"), MOCK_SERVER_PORT);
    processor = new StreamProcessor(client);
  }

  @After
  public void after() throws Exception {
    server.shutdown();
  }


  @Test
  public void testRetry() {

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .batchLimit(2)
        .streamLimit(5)
        .streamLimit(1);

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    processor.start();

  }


  @Test
  public void acceptEncodingGzipIsDefaulted() throws Exception {

    /*
    check that okhttp transparently sets the header Accept-Encoding: gzip and handles decompression.
     */

    Buffer gzipBatch = gzip(batch);
    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(gzipBatch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
        .setHeader("Content-Encoding", "gzip")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .batchLimit(2)
        .streamLimit(5)
        .streamLimit(1);

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    processor.start();
    Thread.sleep(1000L);
    processor.stop();

    RecordedRequest request = server.takeRequest();
    TestCase.assertEquals("gzip", request.getHeaders().get("Accept-Encoding"));
  }

  @Test
  public void customHeadersWithEventStream() throws Exception {

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .batchLimit(2)
        .streamLimit(5)
        .streamLimit(1);
    /*
    check setting the header doesn't mess things up; okhttp handles gzip transparently,
     by always setting Accept-Encoding: gzip. If you set the header you have to handle
     decompression manually. this tests we don't manually set gzip and stay with transparent
     handling
     */
    sc.requestHeader("Accept-Encoding", "gzip");

    Buffer gzipBatch = gzip(batch);
    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(gzipBatch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
        .setHeader("Content-Encoding", "gzip")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .build();

    StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    processor.start();
    Thread.sleep(1000L);
    processor.stop();

    RecordedRequest request = server.takeRequest();
    TestCase.assertEquals("gzip", request.getHeaders().get("Accept-Encoding"));

    // now check subs
    sc = new StreamConfiguration()
        .subscriptionId("ca311263-14e2-448a-a68e-91d50b13fec1")
        .batchLimit(2)
        .streamLimit(5)
        .streamLimit(1);

    sc.requestHeader("Accept-Encoding", "gzip");
    StreamProcessor processor1 = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    StreamProcessor spy = spy(processor1);
    doReturn("foo").when(spy).findEventTypeNameForSubscription(any());

    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(gzipBatch)
        .setHeader("X-Nakadi-StreamId", "nnn")
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
        .setHeader("Content-Encoding", "gzip")
    );
    // this is for the checkpointer
    server.enqueue(new MockResponse().setResponseCode(204));

    spy.start();
    Thread.sleep(1000L);
    spy.stop();

    request = server.takeRequest();
    TestCase.assertEquals("gzip", request.getHeaders().get("Accept-Encoding"));
  }

  @Test
  public void defaultScope() {

    final boolean[] askedForToken = {false};

    NakadiClient client =
        NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
                askedForToken[0] = true;
              }
              return Optional.empty();
            })
            .build();

    StreamConfiguration sc = new StreamConfiguration().subscriptionId("s1");
    StreamProcessor sp = spy(StreamProcessor.newBuilder(client)
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build());

    Resource r = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(sp.buildResource(sc)).thenReturn(r);

    // just invoke the resource supplier part of the observable, it's where we open the stream

    final Callable<Response> resourceFactory =
        sp.buildResourceFactory(new StreamConfiguration().subscriptionId("sub1"));

    try {
      resourceFactory.call();
    } catch (Exception ignored) {
    }

    // check our stream proc was scoped
    ArgumentCaptor<ResourceOptions> options1 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(sp, times(1)).requestStreamConnection(
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options1.capture(),
        any());
    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options1.getValue().scope());

    // check out underlying resource was scoped
    ArgumentCaptor<ResourceOptions> options2 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options2.capture());
    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options2.getValue().scope());

    // check our token provider was asked for the right scope
    assertTrue(askedForToken[0]);
  }

  @Test
  public void customScope() {

    final boolean[] askedForToken = {false};
    String customScope = "custom";

    NakadiClient client =
        NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (customScope.equals(scope)) {
                askedForToken[0] = true;
              }
              return Optional.empty();
            })
            .build();

    StreamConfiguration sc = new StreamConfiguration().subscriptionId("s1");
    StreamProcessor sp = spy(StreamProcessor.newBuilder(client)
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .scope(customScope)
        .build());

    Resource r = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(sp.buildResource(sc)).thenReturn(r);

    // just invoke the resource supplier part of the observable, it's where we open the stream
    Callable<Response> resourceFactory =
        sp.buildResourceFactory(new StreamConfiguration().subscriptionId("sub1"));

    try {
      resourceFactory.call();
    } catch (Exception ignored) {
    }

    // check our stream proc was scoped
    ArgumentCaptor<ResourceOptions> options1 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(sp, times(1)).requestStreamConnection(
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options1.capture(),
        any());
    assertEquals(customScope, options1.getValue().scope());

    // check out underlying resource was scoped
    ArgumentCaptor<ResourceOptions> options2 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options2.capture());
    assertEquals(customScope, options2.getValue().scope());

    // check our token provider was asked for the right scope
    assertTrue(askedForToken[0]);
  }


  @Test
  public void testOffsetObservers() {

    StreamProcessor sp = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp.streamOffsetObserver() instanceof SubscriptionOffsetObserver);

    sp = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().eventTypeName("e1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp.streamOffsetObserver() instanceof LoggingStreamOffsetObserver);
  }

  @Test
  public void testBuilder() {

    try {
      StreamProcessor.newBuilder(client).build();
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Please provide a stream configuration", e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client).streamConfiguration(new StreamConfiguration()).build();
      fail();
    } catch (NakadiException e) {
      assertEquals("Please supply either a subscription id or an event type;  (400)",
          e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client)
          .streamConfiguration(new StreamConfiguration().eventTypeName("et").subscriptionId("s1"))
          .build();
      fail();
    } catch (NakadiException e) {
      assertEquals(
          "Cannot be configured with both a subscriptionId and an eventTypeName; subscriptionId=s1 eventTypeName=et (400)",
          e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client)
          .streamConfiguration(new StreamConfiguration().eventTypeName("et"))
          .build();
      fail("expecting IllegalArgumentException for missing StreamObserver on event stream");
    } catch (IllegalArgumentException e) {
      assertEquals("Please provide a StreamObserverProvider", e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client)
          .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
          .build();
      fail("expecting IllegalArgumentException for missing StreamObserver on subscription stream");
    } catch (IllegalArgumentException e) {
      assertEquals("Please provide a StreamObserverProvider", e.getMessage());
    }

    final StreamProcessor sp = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().eventTypeName("et"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertNotNull(sp);

    SubscriptionOffsetCheckpointer checkpointer = new SubscriptionOffsetCheckpointer(client, true);
    final StreamProcessor sp1 = StreamProcessor.newBuilder(client)
        .checkpointer(checkpointer)
        .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp1.streamOffsetObserver() instanceof SubscriptionOffsetObserver);
    assertEquals(checkpointer,
        ((SubscriptionOffsetObserver) sp1.streamOffsetObserver()).checkpointer());
    assertEquals(true, ((SubscriptionOffsetObserver) sp1.streamOffsetObserver()).checkpointer()
        .suppressInvalidSessionException());

    final StreamProcessor sp2 = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();
  }

  @Test
  public void buildWithAndWithoutSuppression() {

    final SubscriptionOffsetCheckpointer checkpointer1 =
        new SubscriptionOffsetCheckpointer(client)
            .suppressNetworkException(true)
            .suppressInvalidSessionException(true);

    final StreamProcessor sp1 = StreamProcessor.newBuilder(client)
        .checkpointer(checkpointer1)
        .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp1.streamOffsetObserver() instanceof SubscriptionOffsetObserver);
    final SubscriptionOffsetObserver observer1 =
        (SubscriptionOffsetObserver) sp1.streamOffsetObserver();

    assertTrue(null != observer1.checkpointer());
    assertEquals(true,  observer1.checkpointer().suppressInvalidSessionException());
    assertEquals(true,  observer1.checkpointer().suppressNetworkException());

    SubscriptionOffsetCheckpointer checkpointer2 = new SubscriptionOffsetCheckpointer(client);
    final StreamProcessor sp2 = StreamProcessor.newBuilder(client)
        .checkpointer(checkpointer2)
        .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp2.streamOffsetObserver() instanceof SubscriptionOffsetObserver);
    final SubscriptionOffsetObserver observer2 =
        (SubscriptionOffsetObserver) sp2.streamOffsetObserver();

    assertTrue(null != observer2.checkpointer());
    assertEquals(false,  observer2.checkpointer().suppressInvalidSessionException());
    assertEquals(false,  observer2.checkpointer().suppressNetworkException());
  }

  @Test
  public void testBuilderSubscriptionOffsetPublisher() throws Exception {

    final SubscriptionOffsetCheckpointer checkpointer1 = new SubscriptionOffsetCheckpointer(client, true);
    SubscriptionOffsetObserver observer = new SubscriptionOffsetObserver(checkpointer1);
    observer = spy(observer);

    final SubscriptionOffsetPublisher publisher = SubscriptionOffsetPublisher.create();
    publisher.subscribe(observer);

    final StreamProcessor sp3 = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
        .streamOffsetObserver(publisher)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp3.streamOffsetObserver() instanceof SubscriptionOffsetPublisher);

    final SubscriptionOffsetPublisher foundPublisher =
        (SubscriptionOffsetPublisher) sp3.streamOffsetObserver();

    final HashMap<String, String> context = Maps.newHashMap();
    context.put(StreamResourceSupport.X_NAKADI_STREAM_ID, "aa");
    context.put(StreamResourceSupport.SUBSCRIPTION_ID, "bb");
    final StreamCursorContext streamCursorContext =
        new StreamCursorContextReal(new Cursor("p", "o", "e"), context);


    when(observer.checkpointer()).thenReturn(mock(SubscriptionOffsetCheckpointer.class));
    foundPublisher.onNext(streamCursorContext);
    // because I'm wicked and I'm lazy. todo: replace with a latch or something more certain
    Thread.sleep(1200);
    verify(observer, times(1)).onNext(streamCursorContext);
  }

  private Buffer gzip(String data) throws IOException {
    Buffer result = new Buffer();
    BufferedSink sink = Okio.buffer(new GzipSink(result));
    sink.writeUtf8(data);
    sink.close();
    return result;
  }
}