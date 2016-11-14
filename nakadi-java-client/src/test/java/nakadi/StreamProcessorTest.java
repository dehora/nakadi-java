package nakadi;

import java.util.Optional;
import okhttp3.OkHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import rx.functions.Func0;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class StreamProcessorTest {

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:9080").build();
  private StreamProcessor processor;

  @Before
  public void before() {
    processor = new StreamProcessor(client);
  }

  @Test
  public void defaultScope() {

    final boolean[] askedForToken = {false};

    NakadiClient client =
        NakadiClient.newBuilder()
            .baseURI("http://localhost:9080")
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
    Func0<Response> resourceFactory =
        sp.resourceFactory(new StreamConfiguration().subscriptionId("sub1"));

    try {
      resourceFactory.call();
    } catch (NetworkException | NotFoundException ignored) {
    }

    // check our stream proc was scoped
    ArgumentCaptor<ResourceOptions> options1 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(sp, times(1)).requestStreamConnection(
        Matchers.eq("http://localhost:9080/subscriptions/sub1/events"),
        options1.capture(),
        any());
    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options1.getValue().scope());

    // check out underlying resource was scoped
    ArgumentCaptor<ResourceOptions> options2 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sub1/events"),
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
            .baseURI("http://localhost:9080")
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
    Func0<Response> resourceFactory =
        sp.resourceFactory(new StreamConfiguration().subscriptionId("sub1"));

    try {
      resourceFactory.call();
    } catch (NetworkException | NotFoundException ignored) {
    }

    // check our stream proc was scoped
    ArgumentCaptor<ResourceOptions> options1 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(sp, times(1)).requestStreamConnection(
        Matchers.eq("http://localhost:9080/subscriptions/sub1/events"),
        options1.capture(),
        any());
    assertEquals(customScope, options1.getValue().scope());

    // check out underlying resource was scoped
    ArgumentCaptor<ResourceOptions> options2 =
        ArgumentCaptor.forClass(ResourceOptions.class);
    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sub1/events"),
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
          "Cannot be configured with both a subscription id or an event type; subscriptionId=s1 eventTypeName=et (400)",
          e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client)
          .streamConfiguration(new StreamConfiguration().eventTypeName("et"))
          .build();
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Please provide an observer factory", e.getMessage());
    }

    final StreamProcessor sp = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().eventTypeName("et"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertNotNull(sp);
  }
}