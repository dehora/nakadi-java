package nakadi;

import java.util.Optional;
import okhttp3.OkHttpClient;
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

public class EventTypeResourceRealTest {

  @Test
  public void createWithScope() {

    EventType et2 = buildEventType();

    final boolean[] askedForNAKADI_EVENT_TYPE_WRITE = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .tokenProvider(scope -> {
          if (TokenProvider.NAKADI_EVENT_TYPE_WRITE.equals(scope)) {
            askedForNAKADI_EVENT_TYPE_WRITE[0] = true;
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
      new EventTypeResourceReal(client)
          .update(et2);
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.PUT),
        Matchers.eq("http://localhost:9080/event-types/" + et2.name()),
        options.capture(),
        Matchers.eq(et2)
    );

    assertEquals(TokenProvider.NAKADI_EVENT_TYPE_WRITE, options.getValue().scope());
    assertTrue(askedForNAKADI_EVENT_TYPE_WRITE[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      new EventTypeResourceReal(client)
          .scope(customScope)
          .update(et2);
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.PUT),
        Matchers.eq("http://localhost:9080/event-types/" + et2.name()),
        options.capture(),
        Matchers.eq(et2)
    );

    assertEquals(customScope, options.getValue().scope());
    assertTrue(askedForCustomToken[0]);
  }

  @Test
  public void updateWithScope() {

    EventType et2 = buildEventType();

    final boolean[] askedForNAKADI_EVENT_TYPE_WRITE = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .tokenProvider(scope -> {
          if (TokenProvider.NAKADI_EVENT_TYPE_WRITE.equals(scope)) {
            askedForNAKADI_EVENT_TYPE_WRITE[0] = true;
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
      new EventTypeResourceReal(client)
          .create(et2);
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/event-types"),
        options.capture(),
        Matchers.eq(et2)
    );

    assertEquals(TokenProvider.NAKADI_EVENT_TYPE_WRITE, options.getValue().scope());
    assertTrue(askedForNAKADI_EVENT_TYPE_WRITE[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      new EventTypeResourceReal(client)
          .scope(customScope)
          .create(et2);
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/event-types"),
        options.capture(),
        Matchers.eq(et2)
    );

    assertEquals(customScope, options.getValue().scope());
    assertTrue(askedForCustomToken[0]);
  }

  private EventType buildEventType() {
    return new EventType()
        .category(EventType.Category.data)
        .name("et-1-" + System.currentTimeMillis() / 1000)
        .owningApplication("acme")
        .partitionStrategy(EventType.PARTITION_HASH)
        .enrichmentStrategy(EventType.ENRICHMENT_METADATA)
        .partitionKeyFields("id")
        .schema(new EventTypeSchema().schema(
            "{ \"properties\": { \"id\": { \"type\": \"string\" } } }"));
  }

  @Test
  public void listWithScope() {

    final boolean[] askedForNAKADI_EVENT_STREAM_READ = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .tokenProvider(scope -> {
          if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
            askedForNAKADI_EVENT_STREAM_READ[0] = true;
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
      new EventTypeResourceReal(client).list();
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/event-types"),
        options.capture()
    );

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options.getValue().scope());
    assertTrue(askedForNAKADI_EVENT_STREAM_READ[0]);
    assertFalse(askedForCustomToken[0]);

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      new EventTypeResourceReal(client).scope(customScope).list();
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/event-types"),
        options.capture());

    assertEquals(customScope, options.getValue().scope());
    assertTrue(askedForCustomToken[0]);
  }

  @Test
  public void findWithScope() {

    final boolean[] askedForNAKADI_EVENT_STREAM_READ = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .tokenProvider(scope -> {
          if (TokenProvider.NAKADI_EVENT_STREAM_READ.equals(scope)) {
            askedForNAKADI_EVENT_STREAM_READ[0] = true;
          }

          if (customScope.equals(scope)) {
            askedForCustomToken[0] = true;
          }

          return Optional.empty();
        })
        .build());

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      assertFalse(askedForCustomToken[0]);
      assertFalse(askedForNAKADI_EVENT_STREAM_READ[0]);

      new EventTypeResourceReal(client)
          .findByName("foo");
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/event-types/foo"),
        options.capture(),
        Matchers.eq(EventType.class));

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_READ, options.getValue().scope());
    assertFalse(askedForCustomToken[0]);
    assertTrue(askedForNAKADI_EVENT_STREAM_READ[0]);

    Resource r2 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r2);

    try {
      askedForCustomToken[0] = false;
      assertFalse(askedForCustomToken[0]);

      new EventTypeResourceReal(client)
          .scope(customScope)
          .findByName("foo");
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r2, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9080/event-types/foo"),
        options.capture(),
        Matchers.eq(EventType.class));

    assertEquals(customScope, options.getValue().scope());
    assertTrue(askedForCustomToken[0]);
  }

  @Test
  public void deleteWithScope() {

    final boolean[] askedForNAKADI_CONFIG_WRITE = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .tokenProvider(scope -> {
          if (TokenProvider.NAKADI_CONFIG_WRITE.equals(scope)) {
            askedForNAKADI_CONFIG_WRITE[0] = true;
          }

          if (customScope.equals(scope)) {
            askedForCustomToken[0] = true;
          }

          return Optional.empty();
        })
        .build());

    Resource r1 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r1);

    try {
      assertFalse(askedForCustomToken[0]);
      assertFalse(askedForNAKADI_CONFIG_WRITE[0]);

      new EventTypeResourceReal(client).delete("foo");
    } catch (NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9080/event-types/foo"),
        options.capture());

    assertEquals(TokenProvider.NAKADI_CONFIG_WRITE, options.getValue().scope());
    assertFalse(askedForCustomToken[0]);
    assertTrue(askedForNAKADI_CONFIG_WRITE[0]);

    Resource r2 = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r2);

    try {
      askedForCustomToken[0] = false;
      assertFalse(askedForCustomToken[0]);

      new EventTypeResourceReal(client)
          .scope(customScope)
          .delete("foo");
    } catch (NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r2, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9080/event-types/foo"),
        options.capture());

    assertEquals(customScope, options.getValue().scope());
    assertTrue(askedForCustomToken[0]);
  }
}