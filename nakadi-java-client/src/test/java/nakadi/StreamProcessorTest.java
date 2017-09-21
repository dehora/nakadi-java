package nakadi;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
    processor = new StreamProcessor(client, new StreamProcessorRequestFactory(client));
  }

  @After
  public void after() throws Exception {
    server.shutdown();
  }

  @Test
  public void retriesRuntimeExceptions() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;
    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);

    final LoggingStreamObserverProvider provider = runtimeExceptionProvider("retriesRuntimeExceptions");

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .build();

    processor.start();

    assertTrue(processor.running());
    assertFalse(processor.stopped());

    Thread.sleep(3000L); // enough time for the observer to throw

    processor.stop();

    while (processor.running()) {
      Thread.sleep(100L);
    }

    assertFalse(processor.running());
    assertTrue(processor.stopped());

    assertFalse(processor.failedProcessorException().isPresent());
  }

  @Test
  public void canTrackIllegalStateExceptions() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;
    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);

    final LoggingStreamObserverProvider provider = illegalStateExceptionProvider("canTrackIllegalStateExceptions");

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .build();

    processor.start();

    assertTrue(processor.running());
    assertFalse(processor.stopped());

    Thread.sleep(1000L);

    while (processor.running()) {
      Thread.sleep(100L);
    }

    assertFalse(processor.running());
    assertTrue(processor.stopped());

    //noinspection ConstantConditions
    assertTrue(processor.failedProcessorException().get() instanceof IllegalStateException);
  }

  @Test
  public void canTrackNonRetryables() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;
    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);

    final LoggingStreamObserverProvider provider = nonRetryableNakadiExceptionProvider("canTrackNonRetryables");

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .build();

    processor.start();

    assertTrue(processor.running());
    assertFalse(processor.stopped());

    Thread.sleep(1000L);

    while (processor.running()) {
      Thread.sleep(100L);
    }

    assertFalse(processor.running());
    assertTrue(processor.stopped());

    //noinspection ConstantConditions
    assertTrue(processor.failedProcessorException().get() instanceof NonRetryableNakadiException);
  }


  @Test
  public void canTrackErrors() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;
    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);

    final LoggingStreamObserverProvider provider = oomProvider("canTrackErrors");

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .build();

    processor.start();

    assertTrue(processor.running());
    assertFalse(processor.stopped());

    while (processor.running()) {
      Thread.sleep(100L);
    }

    assertFalse(processor.running());
    assertTrue(processor.stopped());

    //noinspection ConstantConditions
    assertTrue(processor.failedProcessorException().get() instanceof OutOfMemoryError);
  }

  @Test
  public void canTrackWhenRunning() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;
    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);

    final LoggingStreamObserverProvider provider = noopProvider("canTrackWhenRunning");

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .build();

    processor.start();

    assertTrue(processor.running());
    assertFalse(processor.stopped());

    Thread.sleep(800L);
    processor.stop();

    while (processor.running()) {
      Thread.sleep(100L);
    }

    assertFalse(processor.running());
    assertTrue(processor.stopped());
  }

  @Test
  public void consumerDoesNotRetryErrors() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);


    CountDownLatch latch = new CountDownLatch(1);

    final boolean[] raised = {false};

    final LoggingStreamObserverProvider provider =
        new LoggingStreamObserverProvider() {
          @Override public StreamObserver<String> createStreamObserver() {
            return new LoggingStreamObserver() {

              /*
              expecting the following:
              1: onNext is called and throws Error
              2: the consumer pipeline won't retry due to the Error
              3: The observer's onError is called and we mark it
              4: The observer's onStop is called and we release the latch
               */

              @Override public void onError(Throwable e) {
                raised[0] = true;
              }

              @Override public void onNext(StreamBatchRecord<String> record) {
                throw new Error();
              }

              @Override public void onStop() {
                latch.countDown();
              }
            };
          }
        };

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .build();

    processor.start();
    Thread.sleep(1000L);
    latch.await(8, TimeUnit.SECONDS);
    assertTrue("Expecting NonRetryableNakadiException to not be retryable",  raised[0]);
  }

  @Test
  public void consumerDoesNotRetryNonRetryableNakadiException() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);

    CountDownLatch latch = new CountDownLatch(1);

    final boolean[] raised = {false};

    final LoggingStreamObserverProvider provider =
        new LoggingStreamObserverProvider() {
          @Override public StreamObserver<String> createStreamObserver() {
            return new LoggingStreamObserver() {

              /*
              expecting the following:
              1: onNext is called and throws NonRetryableNakadiException
              2: the consumer pipeline won't retry due to the NonRetryableNakadiException
              3: The observer's onError is called and we mark it
              4: The observer's onStop is called and we release the latch
               */

              @Override public void onError(Throwable e) {
                raised[0] = true;
              }

              @Override public void onNext(StreamBatchRecord<String> record) {
                throw new NonRetryableNakadiException(Problem.localProblem("nope", "nope"));
              }

              @Override public void onStop() {
                latch.countDown();
              }
            };
          }
        };

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .build();

    processor.start();
    Thread.sleep(1000L);
    latch.await(8, TimeUnit.SECONDS);
    assertTrue("Expecting NonRetryableNakadiException to not be retryable",  raised[0]);
  }

  @Test
  public void consumerDoesNotRetryErrorsFromStreamConnection() throws Exception {

    String baseURI = "http://localhost:";

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);


       /*
      expecting the following:
      1: onCall is called and throws Error
      2: the consumer pipeline won't retry due to the Error
      3: The observer's onError is called and we mark it
      4: The observer's onStop is called and we release the latch
       */

    StreamProcessorRequestFactory factory = new StreamProcessorRequestFactory(client) {
      @Override Response onCall(StreamConfiguration sc, StreamProcessor sp) throws Exception {
        throw new Error("nope");
      }
    };

    CountDownLatch latch = new CountDownLatch(1);

    final boolean[] raised = {false};

    final LoggingStreamObserverProvider provider =
        new LoggingStreamObserverProvider() {
          @Override public StreamObserver<String> createStreamObserver() {
            return new LoggingStreamObserver() {
              @Override public void onError(Throwable e) {
                raised[0] = true;
              }

              @Override public void onStop() {
                latch.countDown();
              }
            };
          }
        };

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .streamProcessorRequestFactory(factory)
        .build();

    processor.start();
    Thread.sleep(1000L);
    latch.await(8, TimeUnit.SECONDS);
    assertTrue("Expecting Error from stream connection to not be retryable",  raised[0]);
  }

  @Test
  public void consumerCanRetryExceptionsFromStreamConnection() throws Exception {

    String baseURI = "http://localhost:";

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(3, TimeUnit.SECONDS);

    StreamProcessorRequestFactory factory = new StreamProcessorRequestFactory(client) {
      @Override Response onCall(StreamConfiguration sc, StreamProcessor sp) throws Exception {
        throw new Exception("nope");
      }
    };

    CountDownLatch latch = new CountDownLatch(1);

    final boolean[] raised = {false};

    final LoggingStreamObserverProvider provider =
        new LoggingStreamObserverProvider() {
          @Override public StreamObserver<String> createStreamObserver() {
            return new LoggingStreamObserver() {
              @Override public void onError(Throwable e) {
                raised[0] = true;
              }

              @Override public void onStop() {
                latch.countDown();
              }
            };
          }
        };

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(provider)
        .streamProcessorRequestFactory(factory)
        .build();

    processor.start();
    Thread.sleep(1000L);
    latch.await(8, TimeUnit.SECONDS);
    assertFalse("Expecting Exception to be retryable",  raised[0]);
  }

  @Test
  public void consumerCanRetryExceptionsFromStreamConnectionWithMaxRetryAttempts() throws Exception {

    String baseURI = "http://acme";

    NakadiClient client = NakadiClient.newBuilder().baseURI(baseURI).build();

    final int retryAttempts = 3;
    StreamConfiguration sc = new StreamConfiguration()
            .eventTypeName("foo")
            .maxRetryAttempts(retryAttempts)
            .connectTimeout(3, TimeUnit.SECONDS)
            .readTimeout(3, TimeUnit.SECONDS);

    StreamProcessorRequestFactory factory = new StreamProcessorRequestFactory(client) {
      @Override Response onCall(StreamConfiguration sc, StreamProcessor sp) throws Exception {
        throw new Exception("nope");
      }
    };

    CountDownLatch latch = new CountDownLatch(retryAttempts);
    AtomicInteger startCounter = new AtomicInteger(0);
    final boolean[] raised = {false};

    final LoggingStreamObserverProvider provider =
            new LoggingStreamObserverProvider() {
              @Override public StreamObserver<String> createStreamObserver() {
                return new LoggingStreamObserver() {
                  @Override public void onError(Throwable e) {
                    raised[0] = true;
                  }

                  @Override public void onStart() {
                    startCounter.incrementAndGet();
                    latch.countDown();
                  }

                  @Override public void onStop() {
                  }
                };
              }
            };

    final StreamProcessor processor = client.resources()
            .streamBuilder()
            .streamConfiguration(sc)
            .streamObserverFactory(provider)
            .streamProcessorRequestFactory(factory)
            .build();

    processor.start();
    latch.await(2, TimeUnit.MINUTES);

    assertTrue(
        "Expecting Exception to be retried " + retryAttempts + " times " + startCounter.get(),
        startCounter.get() == retryAttempts);
    assertFalse("Expecting Exception to be retryable",  raised[0]);
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
  public void acceptEncodingIdentityIsAccepted() throws Exception {

    server.enqueue(new MockResponse().setResponseCode(200)
        .setBody(batch)
        .setHeader("Content-Type", "application/x-json-stream;charset=UTF-8")
        .setHeader("Content-Encoding", "identity")
    );

    String baseURI = "http://localhost:" + MOCK_SERVER_PORT;

    NakadiClient client = NakadiClient.newBuilder()
        .baseURI(baseURI)
        .enableHttpLogging()
        .build();

    StreamConfiguration sc = new StreamConfiguration()
        .eventTypeName("foo")
        .batchLimit(2)
        .streamLimit(5)
        .streamLimit(1)
         // tell okhttp to ask the server for unencoded data
        .requestHeader("Accept-Encoding", "identity")
        ;

    final StreamProcessor processor = client.resources()
        .streamBuilder()
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    processor.start();
    Thread.sleep(1000L);
    processor.stop();

    RecordedRequest request = server.takeRequest();
    TestCase.assertEquals("identity", request.getHeaders().get("Accept-Encoding"));
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
  public void defaultNoScope() {

    NakadiClient client =
        NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (scope != null) {
                throw new AssertionError("scope should not be called");
              }
              return Optional.empty();
            })
            .build();

    StreamConfiguration sc = new StreamConfiguration().subscriptionId("s1");
    StreamProcessorRequestFactory factory = spy(new StreamProcessorRequestFactory(client));
    StreamProcessor sp = spy(StreamProcessor.newBuilder(client)
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .streamProcessorRequestFactory(factory)
        .build());

    Resource r = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(factory.buildResource(sc)).thenReturn(r);

    // just invoke the resource supplier part of the observable, it's where we open the stream

    final Callable<Response> resourceFactory =
        sp.resourceFactory(new StreamConfiguration().subscriptionId("sub1"));

    try {
      resourceFactory.call();
    } catch (Exception ignored) {
    }

    // check our stream proc was scoped
    ArgumentCaptor<ResourceOptions> options1 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(factory, times(1)).requestStreamConnection(
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options1.capture(),
        any());
    assertNull(options1.getValue().scope());

    // check out underlying resource was scoped
    ArgumentCaptor<ResourceOptions> options2 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options2.capture());
    assertNull(options2.getValue().scope());
  }

  @Test
  public void customNoScope() {

    NakadiClient client =
        NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (scope != null) {
                throw new AssertionError("scope should not be called");
              }
              return Optional.empty();
            })
            .build();

    StreamConfiguration sc = new StreamConfiguration().subscriptionId("s1");
    StreamProcessorRequestFactory factory = spy(new StreamProcessorRequestFactory(client));
    StreamProcessor sp = spy(StreamProcessor.newBuilder(client)
        .streamConfiguration(sc)
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .streamProcessorRequestFactory(factory)
        .build());

    Resource r = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(factory.buildResource(sc)).thenReturn(r);

    // just invoke the resource supplier part of the observable, it's where we open the stream
    Callable<Response> resourceFactory =
        sp.resourceFactory(new StreamConfiguration().subscriptionId("sub1"));

    try {
      resourceFactory.call();
    } catch (Exception ignored) {
    }

    // check our stream proc was scoped
    ArgumentCaptor<ResourceOptions> options1 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(factory, times(1)).requestStreamConnection(
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options1.capture(),
        any());
    assertNull(options1.getValue().scope());

    // check out underlying resource was scoped
    ArgumentCaptor<ResourceOptions> options2 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sub1/events"),
        options2.capture());
    assertNull(options2.getValue().scope());
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

    SubscriptionOffsetCheckpointer checkpointer = new SubscriptionOffsetCheckpointer(client)
        .suppressInvalidSessionException(true);
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

    final SubscriptionOffsetCheckpointer checkpointer1 = new SubscriptionOffsetCheckpointer(client)
        .suppressInvalidSessionException(true);
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

  private LoggingStreamObserverProvider noopProvider(String key) {
    return new LoggingStreamObserverProvider() {
      @Override public StreamObserver<String> createStreamObserver() {
        return new LoggingStreamObserver() {

          @Override public void onError(Throwable e) {
            System.out.println(key + " onError " + e.getMessage());
          }

          @Override public void onNext(StreamBatchRecord<String> record) {
            System.out.println(key + " onNext");
          }

          @Override public void onStop() {
            System.out.println(key + " onStop");
          }
        };
      }
    };
  }

  private LoggingStreamObserverProvider oomProvider(String key) {
    return new LoggingStreamObserverProvider() {
      @Override public StreamObserver<String> createStreamObserver() {
        return new LoggingStreamObserver() {

          @Override public void onError(Throwable e) {
            System.out.println(key + " onError " + e.getMessage());
          }

          @Override public void onNext(StreamBatchRecord<String> record) {
            System.out.println(key + " onNext");
            throw new OutOfMemoryError(key +" oomProvider");
          }

          @Override public void onStop() {
            System.out.println(key + " onStop");
          }
        };
      }
    };
  }

  private LoggingStreamObserverProvider nonRetryableNakadiExceptionProvider(String key) {
    return new LoggingStreamObserverProvider() {
      @Override public StreamObserver<String> createStreamObserver() {
        return new LoggingStreamObserver() {

          @Override public void onError(Throwable e) {
            System.out.println(key + " onError " + e.getMessage());
          }

          @Override public void onNext(StreamBatchRecord<String> record) {
            System.out.println(key + " onNext");
            throw new NonRetryableNakadiException(Problem.localProblem(key," nonRetryableNakadiExceptionProvider"));
          }

          @Override public void onStop() {
            System.out.println(key + " onStop");
          }
        };
      }
    };
  }

  private LoggingStreamObserverProvider illegalStateExceptionProvider(String key) {
    return new LoggingStreamObserverProvider() {
      @Override public StreamObserver<String> createStreamObserver() {
        return new LoggingStreamObserver() {

          @Override public void onError(Throwable e) {
            System.out.println(key + " @@@ onError " + e.getMessage());
          }

          @Override public void onNext(StreamBatchRecord<String> record) {
            System.out.println(key + " onNext");
            throw new IllegalStateException(key +" illegalStateExceptionProvider");
          }

          @Override public void onStop() {
            System.out.println(key + " onStop");
          }
        };
      }
    };
  }

  private LoggingStreamObserverProvider runtimeExceptionProvider(String key) {
    return new LoggingStreamObserverProvider() {
      @Override public StreamObserver<String> createStreamObserver() {
        return new LoggingStreamObserver() {

          @Override public void onError(Throwable e) {
            System.out.println(key + " @@@ onError " + e.getMessage());
          }

          @Override public void onNext(StreamBatchRecord<String> record) {
            System.out.println(key + " onNext");
            throw new RuntimeException(key +" runtimeExceptionProvider");
          }

          @Override public void onStop() {
            System.out.println(key + " onStop");
          }
        };
      }
    };
  }
}
