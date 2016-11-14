package nakadi;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubscriptionResourceRealTest {

  @Test
  public void listSansRetry() {
    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9080")
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    verify(r0, times(1)).executeRequest(Matchers.anyObject());
  }

  @Test
  public void listWithRetry() {
    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9080")
            .build());

    OkHttpResource r0 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));


    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r0);

    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.MILLISECONDS)
        .maxAttempts(3)
        .maxInterval(3, TimeUnit.MILLISECONDS)
        .build();

    try {
      new SubscriptionResourceReal(client)
          .retryPolicy(exponentialRetry)
          .list();
    } catch (NetworkException | NotFoundException ignored) {
    }

    verify(r0, times(3)).executeRequest(Matchers.anyObject());
  }

  @Test
  public void listWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9080")
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions"),
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions"),
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
            .baseURI("http://localhost:9080")
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sid"),
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sid"),
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
            .baseURI("http://localhost:9080")
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sid/cursors"),
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sid/cursors"),
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
            .baseURI("http://localhost:9080")
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sid/stats"),
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/subscriptions/sid/stats"),
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
            .baseURI("http://localhost:9080")
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9080/subscriptions/id"),
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
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9080/subscriptions/id"),
        options.capture()
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

  @Test
  public void checkpointWithScope() {
    final boolean[] askedForDefaultToken = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client =
        spy(NakadiClient.newBuilder()
            .baseURI("http://localhost:9080")
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

    Cursor c = new Cursor().cursorToken("ctoken").eventType("et1").offset("0").partition("1");

    Map<String, Object> requestMap = Maps.newHashMap();
    requestMap.put("items", Lists.newArrayList(c));

    HashMap<String, String> context = Maps.newHashMap();
    context.put("X-Nakadi-StreamId", "aa");
    context.put("SubscriptionId", "woo");

    try {

      RetryPolicy backoff = ExponentialRetry.newBuilder()
          .initialInterval(1, TimeUnit.MILLISECONDS)
          .maxAttempts(1)
          .maxInterval(1, TimeUnit.MILLISECONDS)
          .build();

      new SubscriptionResourceReal(client)
          .checkpoint(backoff, context, c);
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/subscriptions/woo/cursors"),
        options.capture(),
        Matchers.eq(requestMap)
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

      RetryPolicy backoff = ExponentialRetry.newBuilder()
          .initialInterval(1, TimeUnit.MILLISECONDS)
          .maxAttempts(1)
          .maxInterval(1, TimeUnit.MILLISECONDS)
          .build();

      SubscriptionResource resource = new SubscriptionResourceReal(client).scope(customScope);

      // call the inner method to control the backoff, the interface method's just a wrapper
      ((SubscriptionResourceReal)resource).checkpoint(backoff, context, c);

    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/subscriptions/woo/cursors"),
        options.capture(),
        Matchers.eq(requestMap)
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
            .baseURI("http://localhost:9080")
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
          .create(subscription);
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/subscriptions"),
        options.capture(),
        Matchers.eq(subscription)
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
          .create(subscription);

    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/subscriptions"),
        options.capture(),
        Matchers.eq(subscription)
    );

    assertEquals(customScope, options.getValue().scope());
    assertFalse(askedForDefaultToken[0]);
    assertTrue(askedForCustomToken[0]);


  }

}