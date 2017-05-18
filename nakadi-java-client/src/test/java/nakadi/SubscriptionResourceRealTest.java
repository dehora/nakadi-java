package nakadi;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import junit.framework.TestCase;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubscriptionResourceRealTest {

  public static final int MOCK_SERVER_PORT = 8317;


  MockWebServer server = new MockWebServer();
  private GsonSupport json = new GsonSupport();

  // these two are not annotated as we don't want to open a server for every test

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
  public void cursorResetApi() throws Exception {

    try {
      before();

      final NakadiClient client = NakadiClient.newBuilder()
          .baseURI("http://localhost:" + MOCK_SERVER_PORT)
          .build();

      server.enqueue(new MockResponse().setResponseCode(204));

      final SubscriptionResource subscriptions = client.resources().subscriptions();

      final String eventTypeName = "blackhole.sun";
      final String subscriptionId = "5-19";
      final String partition = "7";
      final String cursorToken = "octave4";
      final String offset = "000000000019642017";
      final Cursor cursor = new Cursor(partition, offset, eventTypeName).cursorToken(cursorToken);

      subscriptions.reset(subscriptionId, Lists.newArrayList(cursor));
      final RecordedRequest request = server.takeRequest();

      assertEquals("PATCH /subscriptions/5-19/cursors HTTP/1.1", request.getRequestLine());
      assertEquals("application/json; charset=utf8", request.getHeaders().get("Content-Type"));
      assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));

      final String body = request.getBody().readUtf8();
      final SubscriptionResourceReal.CursorResetCollection sent =
          json.fromJson(body, SubscriptionResourceReal.CursorResetCollection.class);
      assertEquals(1, sent.items.size());
      final Cursor sentCursor = sent.items.get(0);
      assertEquals("expect cursor token to be filtered out before sending",
          Optional.empty(), sentCursor.cursorToken());
      assertEquals(partition, sentCursor.partition());
      assertEquals(offset, sentCursor.offset());
      assertEquals(eventTypeName, sentCursor.eventType().get());

    } finally {
      after();
    }
  }

  @Test
  public void listSansRetry() {
    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .build());

    OkHttpResource r0 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));


    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r0);

    try {
      new SubscriptionResourceReal(client)
          .list();
    } catch (RetryableException | NotFoundException ignored) {
    }

    verify(r0, times(1)).okHttpRequest(Matchers.anyObject());
  }

  @Test
  public void listWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
                askedForDefaultToken[0] = true;
              }

              if (customScope.equals(scope)) {
                askedForCustomToken[0] = true;
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

    try {
      new SubscriptionResourceReal(client)
          .list();
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions"),
        options.capture()
    );

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options.getValue().scope());
    assertTrue(askedForDefaultToken[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      askedForDefaultToken[0] = false;
      askedForCustomToken[0] = false;
      assertFalse(askedForDefaultToken[0]);
      assertFalse(askedForCustomToken[0]);

      new SubscriptionResourceReal(client)
          .scope(customScope)
          .list();
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions"),
        options.capture()
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

  @Test
  public void findWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
                askedForDefaultToken[0] = true;
              }

              if (customScope.equals(scope)) {
                askedForCustomToken[0] = true;
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

    try {
      new SubscriptionResourceReal(client)
          .find("sid");
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sid"),
        options.capture(),
        Matchers.eq(Subscription.class)
    );

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options.getValue().scope());
    assertTrue(askedForDefaultToken[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      askedForDefaultToken[0] = false;
      askedForCustomToken[0] = false;
      assertFalse(askedForDefaultToken[0]);
      assertFalse(askedForCustomToken[0]);

      new SubscriptionResourceReal(client)
          .scope(customScope)
          .find("sid");
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sid"),
        options.capture(),
        Matchers.eq(Subscription.class)
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

  @Test
  public void cursorsWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
                askedForDefaultToken[0] = true;
              }

              if (customScope.equals(scope)) {
                askedForCustomToken[0] = true;
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

    try {
      new SubscriptionResourceReal(client)
          .cursors("sid");
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sid/cursors"),
        options.capture()
    );

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options.getValue().scope());
    assertTrue(askedForDefaultToken[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      askedForDefaultToken[0] = false;
      askedForCustomToken[0] = false;
      assertFalse(askedForDefaultToken[0]);
      assertFalse(askedForCustomToken[0]);

      new SubscriptionResourceReal(client)
          .scope(customScope)
          .cursors("sid");
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sid/cursors"),
        options.capture()
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

  @Test
  public void statsWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
                askedForDefaultToken[0] = true;
              }

              if (customScope.equals(scope)) {
                askedForCustomToken[0] = true;
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

    try {
      new SubscriptionResourceReal(client)
          .stats("sid");
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sid/stats"),
        options.capture()
    );

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options.getValue().scope());
    assertTrue(askedForDefaultToken[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      askedForDefaultToken[0] = false;
      askedForCustomToken[0] = false;
      assertFalse(askedForDefaultToken[0]);
      assertFalse(askedForCustomToken[0]);

      new SubscriptionResourceReal(client)
          .scope(customScope)
          .stats("sid");
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/subscriptions/sid/stats"),
        options.capture()
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

  @Test
  public void deleteWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (TokenProvider.NAKADI_CONFIG_WRITE.equals(scope)) {
                askedForDefaultToken[0] = true;
              }

              if (customScope.equals(scope)) {
                askedForCustomToken[0] = true;
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

    try {
      new SubscriptionResourceReal(client)
          .delete("id");
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9081/subscriptions/id"),
        options.capture()
    );

    assertEquals(TokenProvider.NAKADI_CONFIG_WRITE, options.getValue().scope());
    assertTrue(askedForDefaultToken[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      askedForDefaultToken[0] = false;
      askedForCustomToken[0] = false;
      assertFalse(askedForDefaultToken[0]);
      assertFalse(askedForCustomToken[0]);

      new SubscriptionResourceReal(client)
          .scope(customScope)
          .delete("id");
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9081/subscriptions/id"),
        options.capture()
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

  @Test
  public void createWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    Subscription subscription = new Subscription()
        .consumerGroup("mccaffrey-cg")
        .eventType("priority-requisitions")
        .owningApplication("shaper");

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9081")
            .tokenProvider(scope -> {
              if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
                askedForDefaultToken[0] = true;
              }

              if (customScope.equals(scope)) {
                askedForCustomToken[0] = true;
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


    try {

      new SubscriptionResourceReal(client)
          .createReturningResponse(subscription);
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9081/subscriptions"),
        options.capture(),
        Matchers.any(ContentSupplier.class)
    );

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options.getValue().scope());
    assertTrue(askedForDefaultToken[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      askedForDefaultToken[0] = false;
      askedForCustomToken[0] = false;
      assertFalse(askedForDefaultToken[0]);
      assertFalse(askedForCustomToken[0]);


      new SubscriptionResourceReal(client)
          .scope(customScope)
          .createReturningResponse(subscription);

    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9081/subscriptions"),
        options.capture(),
        Matchers.any(ContentSupplier.class)
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

}