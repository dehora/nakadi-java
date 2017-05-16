package nakadi;

import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.util.List;
import java.util.Optional;
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

public class EventTypeResourceRealTest {

  public static final int MOCK_SERVER_PORT = 8316;
  MockWebServer server = new MockWebServer();
  final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:" + MOCK_SERVER_PORT).build();

  public void before() throws Exception {
    server.start(InetAddress.getByName("localhost"), MOCK_SERVER_PORT);
  }

  public void after() throws Exception {
    server.shutdown();
  }

  @Test
  public void testSendRequests() throws Exception {

    try {
      before();
      testShiftRequests();
      testDistanceRequests();
      testLagRequests();
      testPartitionParam();
    } finally {
      after();
    }
  }

  private void testPartitionParam() throws Exception {

    final String json = TestSupport.load("partition-unconsumed-response-ok.json");
    server.enqueue(new MockResponse().setResponseCode(200).setBody(json));

    client.resources()
        .eventTypes()
        .partition("et1", "0", new QueryParams().param("consumed_offset", "000000000000000020"));

    final RecordedRequest request = server.takeRequest();
    assertEquals(
        "/event-types/et1/partitions/0?consumed_offset=000000000000000020",
        request.getPath());

  }

  private void testLagRequests() throws Exception {

    final String json = TestSupport.load("cursor-lag-response-ok.json");
    server.enqueue(new MockResponse().setResponseCode(200).setBody(json));
    Cursor c = new Cursor().partition("0").offset("000000000000000020");

    final PartitionCollection collection =
        client.resources().eventTypes().lag("et1", Lists.newArrayList(c));

    assertEquals(1, collection.items().size());

    Partition p = collection.items().get(0);
    assertEquals("000000000000000029", p.newestAvailableOffset());
    assertEquals("000000000000000020", p.oldestAvailableOffset());
    assertEquals("0", p.partition());
    assertTrue(9L == p.unconsumedEvents());

    final RecordedRequest request = server.takeRequest();
    assertEquals("/event-types/et1/cursors-lag", request.getPath());

    final String sentJson = request.getBody().readUtf8();
    TypeLiteral<List<Cursor>> typeLiteral = new TypeLiteral<List<Cursor>>() {
    };

    final List<Cursor> sentObj = GsonSupport.gson().fromJson(sentJson, typeLiteral.type());
    assertEquals("expected one cursor value to be sent to server",  1, sentObj.size());

    Cursor cSent = sentObj.get(0);
    assertEquals("0", cSent.partition());
    assertEquals("000000000000000020", cSent.offset());
  }

  private void testDistanceRequests() throws Exception {

    final String json = TestSupport.load("cursor-distance-response-ok.json");
    server.enqueue(new MockResponse().setResponseCode(200).setBody(json));

    Cursor i = new Cursor().partition("1").offset("000000000000000021");
    Cursor f = new Cursor().partition("0").offset("000000000000000025");
    CursorDistance cd = new CursorDistance().initialCursor(i).finalCursor(f);

    final CursorDistanceCollection collection =
        client.resources().eventTypes().distance("et1", Lists.newArrayList(cd));

    assertEquals(1, collection.items().size());

    boolean seen0 = false;
    for (CursorDistance cursor : collection.items()) {
      if(cursor.distance() == 5L) {
        seen0 = true;
      }
    }
    assertTrue("expected distance to be marshalled", seen0);

    final RecordedRequest request = server.takeRequest();
    assertEquals("/event-types/et1/cursor-distances", request.getPath());

    final String sentJson = request.getBody().readUtf8();
    TypeLiteral<List<CursorDistance>> typeLiteral = new TypeLiteral<List<CursorDistance>>() {
    };

    final List<CursorDistance> sentObj = GsonSupport.gson().fromJson(sentJson, typeLiteral.type());
    assertEquals("expected one distance value to be sent to server",  1, sentObj.size());
    
    boolean sent = false;
    for (CursorDistance cd1 : sentObj) {
      if(cd1.finalCursor() != null && cd1.initialCursor() != null) {
        sent = true;
      }
    }
    assertTrue("expected two cursor values to be sent to server", sent);
  }

  private void testShiftRequests() throws Exception {

    final String json = TestSupport.load("cursor-shift-response-ok.json");
    server.enqueue(new MockResponse().setResponseCode(200).setBody(json));

    Cursor c0 = new Cursor().partition("0").offset("000000000000000025").shift(-1L);
    Cursor c1 = new Cursor().partition("1").offset("000000000000000021").shift(2L);

    final CursorCollection shift =
        client.resources().eventTypes().shift("et1", Lists.newArrayList(c0, c1));

    assertEquals(2, shift.items().size());

    boolean seen0 = false;
    boolean seen1 = false;

    for (Cursor cursor : shift.items()) {
      if(cursor.partition().equals("0")) {
        seen0 = true;
      }
      if(cursor.partition().equals("1")) {
        seen1 = true;
      }
    }
    assertTrue("expected two partitions to be marshalled", seen0 && seen1);

    final RecordedRequest request = server.takeRequest();
    assertEquals("/event-types/et1/shifted-cursors", request.getPath());

    final String sentJson = request.getBody().readUtf8();
    TypeLiteral<List<Cursor>> typeLiteral = new TypeLiteral<List<Cursor>>() {
    };

    final List<Cursor> sentObj = GsonSupport.gson().fromJson(sentJson, typeLiteral.type());
    boolean sent0 = false;
    boolean sent1 = false;
    for (Cursor cursor : sentObj) {
      if(cursor.shift().equals(-1L)) {
        sent0 = true;
      }
      if(cursor.shift().equals(2L)) {
        sent1 = true;
      }
    }

    assertTrue("expected two shift values to be sent to server", sent0 && sent1);
  }

  @Test
  public void createWithScope() {

    EventType et2 = buildEventType();

    final boolean[] askedForNAKADI_EVENT_TYPE_WRITE = {false};
    final boolean[] askedForCustomToken = {false};
    String customScope = "custom";

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9081")
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.PUT),
        Matchers.eq("http://localhost:9081/event-types/" + et2.name()),
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.PUT),
        Matchers.eq("http://localhost:9081/event-types/" + et2.name()),
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
        .baseURI("http://localhost:9081")
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9081/event-types"),
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9081/event-types"),
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
        .baseURI("http://localhost:9081")
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r0, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/event-types"),
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/event-types"),
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
        .baseURI("http://localhost:9081")
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/event-types/foo"),
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r2, times(1)).requestThrowing(
        Matchers.eq(Resource.GET),
        Matchers.eq("http://localhost:9081/event-types/foo"),
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
        .baseURI("http://localhost:9081")
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r1, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9081/event-types/foo"),
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
    } catch (RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r2, times(1)).requestThrowing(
        Matchers.eq(Resource.DELETE),
        Matchers.eq("http://localhost:9081/event-types/foo"),
        options.capture());

    assertEquals(customScope, options.getValue().scope());
    assertTrue(askedForCustomToken[0]);
  }
}