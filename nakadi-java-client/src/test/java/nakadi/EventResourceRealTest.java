package nakadi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import junit.framework.TestCase;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventResourceRealTest {

  public static final int MOCK_SERVER_PORT = 8317;

  static class Happened {
    String id;

    public Happened(String id) {
      this.id = id;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Happened happened = (Happened) o;
      return Objects.equals(id, happened.id);
    }

    @Override public int hashCode() {
      return Objects.hash(id);
    }
  }

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
  public void returnedWithABatchItemResponseFor422And207() throws Exception {
    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:" + MOCK_SERVER_PORT)
        .build());

    String errJson = TestSupport.load("err_batch_item_response_array.json");

    try {
      before();

      List<UndefinedEventMapped<UndefinedPayload>> list = ImmutableList.of(
          new UndefinedEventMapped<UndefinedPayload>().data(
              new UndefinedPayload("01", "A", "B")),
          new UndefinedEventMapped<UndefinedPayload>().data(
              new UndefinedPayload("02", "C", "D"))
      );

      EventResource resource = client.resources().events();

      // 422 and 207 batch requests shouldn't throw exceptions

      server.enqueue(new MockResponse().setResponseCode(422).setBody(errJson));
      Response r1 = resource.send("ue-1-1479125860", list);
      assertTrue(r1.statusCode() == 422);

      server.enqueue(new MockResponse().setResponseCode(207).setBody(errJson));
      Response r2 = resource.send("ue-1-1479125860", list);
      assertTrue(r2.statusCode() == 207);

      // 422 and 207 discrete requests shouldn't throw exceptions

      server.enqueue(new MockResponse().setResponseCode(422).setBody(errJson));
      Response r3 = resource.send("ue-1-1479125860",
          new UndefinedEventMapped<UndefinedPayload>()
              .data(new UndefinedPayload("01", "A", "B")));
      assertTrue(r3.statusCode() == 422);

      server.enqueue(new MockResponse().setResponseCode(207).setBody(errJson));
      Response r4 = resource.send("ue-1-1479125860",
          new UndefinedEventMapped<UndefinedPayload>()
              .data(new UndefinedPayload("01", "A", "B")));
      assertTrue(r4.statusCode() == 207);

      // 422 and 207 raw requests shouldn't throw exceptions

      String raw = TestSupport.load("event-type-1.json");

      server.enqueue(new MockResponse().setResponseCode(422).setBody(errJson));
      Response r5 = resource.send("ue-1-1479125860", raw);
      assertTrue(r5.statusCode() == 422);

      server.enqueue(new MockResponse().setResponseCode(207).setBody(errJson));
      Response r6 = resource.send("ue-1-1479125860", raw);
      assertTrue(r6.statusCode() == 207);

      // check we can marshal the content

      String s = r6.responseBody().asString();

      Type TYPE_P = new TypeToken<List<BatchItemResponse>>() {
      }.getType();

      List<BatchItemResponse> collection = json.fromJson(s, TYPE_P);

      BatchItemResponseCollection bir = new BatchItemResponseCollection(collection, Lists.newArrayList(), client);

      assertTrue(bir.items().size() == 2);
      BatchItemResponse batchItemResponse = bir.items().get(0);
      assertEquals ("7d7574c3-42ac-4e23-8c92-cd854ab1845a", batchItemResponse.eid());
      assertEquals ("failed", batchItemResponse.publishingStatus().name());
      assertEquals ("enriching", batchItemResponse.step().name());
      assertEquals ("no good", batchItemResponse.detail());

      // check a collection response
      server.enqueue(new MockResponse().setResponseCode(207).setBody(errJson));
      BatchItemResponseCollection batch =
          resource.sendBatch("ue-1-1479125860", Lists.newArrayList(raw));
      assertTrue(batch.items().size() == 2);

      HashSet<String> eids =
          Sets.newHashSet(batch.items().get(0).eid(), batch.items().get(1).eid());
      assertTrue(eids.contains("7d7574c3-42ac-4e23-8c92-cd854ab1845a"));
      assertTrue(eids.contains("980c8aa9-7921-4675-a0c0-0b33b1459944"));
    } finally {
      after();
    }
  }

  @Test
  public void undefinedIsSentToServerMapped() throws Exception {

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:" + MOCK_SERVER_PORT)
        .build());
    EventResource resource = client.resources().events();

    UndefinedPayload up1 = new UndefinedPayload("01", "A", "B");
    UndefinedPayload up2 = new UndefinedPayload("02", "C", "D");

    UndefinedEventMapped<UndefinedPayload> event1 =
        new UndefinedEventMapped<UndefinedPayload>().data(up1);
    UndefinedEventMapped<UndefinedPayload> event2 =
        new UndefinedEventMapped<UndefinedPayload>().data(up2);

    List<UndefinedEventMapped<UndefinedPayload>> list = ImmutableList.of(event1, event2);

    try {
      before();

      server.enqueue(new MockResponse().setResponseCode(200));
      resource.send("ue-1-1479125860", list);
      RecordedRequest request = server.takeRequest();

      assertEquals("POST /event-types/ue-1-1479125860/events HTTP/1.1", request.getRequestLine());
      assertEquals("application/json; charset=utf8", request.getHeaders().get("Content-Type"));
      assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));
      TestCase.assertTrue(request.getHeaders().get("X-Flow-Id") != null);
      TestCase.assertTrue(request.getHeaders().get("X-Client-Platform-Details") != null);

      Type U_TYPE =
          new TypeToken<List<Map<String, String>>>() {
          }.getType();

      Type M_TYPE =
          new TypeToken<Map<String, String>>() {
          }.getType();

      List<Map<String, String>> sent = json.fromJson(request.getBody().readUtf8(), U_TYPE);

      assertEquals(2, sent.size());

      /*
       we're expecting an undefined event type's "data" field to get lifted out to the
       top level of the json doc, so it should match the raw map versions of the enclosed
       up1 and up2 objects
        */
      Map<String, String> first = sent.get(0);
      Map<String, String> second = sent.get(1);

      Map<String, String> mFirst = json.fromJson(json.toJson(up1), M_TYPE);
      Map<String, String> mSecond = json.fromJson(json.toJson(up2), M_TYPE);

      assertEquals(mFirst, first);
      assertEquals(mSecond, second);

    } finally {
      after();
    }
  }

  @Test
  public void businessIsSentToServerMapped() throws Exception {

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:" + MOCK_SERVER_PORT)
        .build());

    EventResource resource = client.resources().events();

    BusinessPayload bp = new BusinessPayload("22", "A", "B");

    BusinessEventMapped<BusinessPayload> event =
        new BusinessEventMapped<BusinessPayload>()
            .metadata(new EventMetadata())
            .data(bp);

    try {
      before();

      server.enqueue(new MockResponse().setResponseCode(200));
      resource.send("be-1-1479125860", event);
      RecordedRequest request = server.takeRequest();

      assertEquals("POST /event-types/be-1-1479125860/events HTTP/1.1", request.getRequestLine());
      assertEquals("application/json; charset=utf8", request.getHeaders().get("Content-Type"));
      assertEquals(NakadiClient.USER_AGENT, request.getHeaders().get("User-Agent"));
      TestCase.assertTrue(request.getHeaders().get("X-Flow-Id") != null);
      TestCase.assertTrue(request.getHeaders().get("X-Client-Platform-Details") != null);

      String body = request.getBody().readUtf8();

      Type U_TYPE =
          new TypeToken<List<Map<String, Object>>>() {
          }.getType();

      List<Map<String, Object>> sent = json.fromJson(body, U_TYPE);
      assertEquals(1, sent.size());

      /*
       we're expecting a business event type's "data" fields to get lifted out to the
       top level of the json doc, so it should match the raw map versions of the enclosed
       BusinessPayload plus a metadata field
        */
      Map<String, Object> businessAsMap = sent.get(0);
      assertEquals(4, businessAsMap.size());
      assertEquals("22", businessAsMap.get("id"));
      assertEquals("A", businessAsMap.get("a"));
      assertEquals("B", businessAsMap.get("b"));
      assertEquals(3, ((Map)businessAsMap.get("metadata")).size());
      assertTrue(((Map)businessAsMap.get("metadata")).containsKey("eid"));
      assertTrue(((Map)businessAsMap.get("metadata")).containsKey("occurred_at"));
      assertTrue(((Map)businessAsMap.get("metadata")).containsKey("flow_id"));
      assertTrue(((Map)businessAsMap.get("metadata")).get("flow_id").toString().startsWith("njc"));

    } finally {
      after();
    }
  }

  @Test
  public void sendWithFlowId() throws Exception {
    final NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:" + MOCK_SERVER_PORT)
        .build());

    final EventResource resource = client.resources().events();

    final BusinessPayload bp = new BusinessPayload("22", "A", "B");

    final BusinessEventMapped<BusinessPayload> event =
        new BusinessEventMapped<BusinessPayload>()
            .metadata(new EventMetadata())
            .data(bp);

    try {
      before();

      server.enqueue(new MockResponse().setResponseCode(200));
      final String flowId = "new flow id";
      resource.flowId(flowId).send("be-1-1479125860", event);
      final RecordedRequest request = server.takeRequest();

      assertEquals(flowId, request.getHeader("x-flow-id"));
    } finally {
      after();
    }
  }

  @Test
  public void sendNoScope() {

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9380")
        .tokenProvider(scope -> {

          if (scope != null) {
            throw new AssertionError("scope should not be called");
          }

          return Optional.empty();
        })
        .build());

    Resource r = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r);

    try {
      new EventResourceReal(client)
          .send("foo", Lists.newArrayList(new Event<Happened>() {
        public Happened data() {
          return new Happened("a");
        }
      }));
    } catch(RetryableException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r, times(1)).postEventsThrowing(
        Matchers.eq("http://localhost:9380/event-types/foo/events"),
        options.capture(),
        Matchers.any(ContentSupplier.class));

    assertNull(options.getValue().scope());


    r = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r);

    try {

      new EventResourceReal(client)
          .send("foo", Lists.newArrayList(new Event<Happened>() {
        public Happened data() {
          return new Happened("a");
        }
      }));
    } catch(RetryableException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r, times(1)).postEventsThrowing(
        Matchers.eq("http://localhost:9380/event-types/foo/events"),
        options.capture(),
        Matchers.any(ContentSupplier.class));

    assertNull(options.getValue().scope());
  }

  @Test
  public void serdesDomain() {
    EventResourceReal eventResource = new EventResourceReal(null, new GsonSupport());

    EventThing et = new EventThing("a", "b");
    EventRecord<EventThing> er = new EventRecord<>("topic", et);

    Object o = eventResource.mapEventRecordToSerdes(er);

    assertEquals(et, o);
  }

  @Test
  public void serdesUndefinedEventMapped() {
    EventResourceReal eventResource = new EventResourceReal(null, new GsonSupport());

    Map<String, Object> uemap = Maps.newHashMap();
    uemap.put("a", "1");
    UndefinedEventMapped<Map<String, Object>> ue =
        new UndefinedEventMapped<Map<String, Object>>().data(uemap);
    EventRecord<UndefinedEventMapped> er = new EventRecord<>("topic", ue);
    Map<String, Object> outmap = (Map<String, Object>) eventResource.mapEventRecordToSerdes(er);
    assertTrue(outmap.size() == 1);
    assertTrue(outmap.containsKey("a"));
    assertEquals("1", outmap.get("a"));
  }

  @Test
  public void serdesBusinessEventMapped() {
    EventResourceReal eventResource = new EventResourceReal(null, new GsonSupport());

    BusinessEventMapped<Map<String, Object>> be = new BusinessEventMapped<>();
    EventMetadata em = new EventMetadata();
    em.eid("eid1");
    Map<String, Object> uemap = Maps.newHashMap();
    uemap.put("a", 1);
    uemap.put("b", 22.0001);
    uemap.put("c", "40.0001");
    uemap.put("d", "c22.0001");
    be.data(uemap);
    be.metadata(em);

    EventRecord<BusinessEventMapped> er = new EventRecord<>("topic", be);
    JsonObject outmap = (JsonObject) eventResource.mapEventRecordToSerdes(er);
    assertTrue(outmap.size() == 5);
    assertTrue(outmap.get("metadata") != null);
    assertTrue(outmap.get("a") != null);
    assertEquals(1, outmap.get("a").getAsInt());
    assertEquals(22.0001, outmap.get("b").getAsDouble(), 0.0d);
    assertEquals("40.0001", outmap.get("c").getAsString());
    assertEquals("c22.0001", outmap.get("d").getAsString());

    final JsonElement metadata = outmap.get("metadata");
    assertEquals(em.eid(), metadata.getAsJsonObject().get("eid").getAsString());
    assertEquals(em.flowId(), metadata.getAsJsonObject().get("flow_id").getAsString());
    assertEquals(
        em.occurredAt(),
        new OffsetDateTimeSerdes().toOffsetDateTime(
            metadata.getAsJsonObject().get("occurred_at").getAsString()));
  }

  static class EventThing implements Event {
    final String a;
    final String b;

    public EventThing(String a, String b) {
      this.a = a;
      this.b = b;
    }

    @Override public Object data() {
      return null;
    }

    @Override public int hashCode() {
      return Objects.hash(a, b);
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EventThing that = (EventThing) o;
      return Objects.equals(a, that.a) &&
          Objects.equals(b, that.b);
    }
  }

  static class UndefinedPayload {
    String id;
    String foo;
    String bar;

    public UndefinedPayload(String id, String foo, String bar) {
      this.id = id;
      this.foo = foo;
      this.bar = bar;
    }

    @Override public String toString() {
      return "UndefinedPayload{" + "id='" + id + '\'' +
          ", foo='" + foo + '\'' +
          ", bar='" + bar + '\'' +
          '}';
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UndefinedPayload that = (UndefinedPayload) o;
      return Objects.equals(id, that.id) &&
          Objects.equals(foo, that.foo) &&
          Objects.equals(bar, that.bar);
    }

    @Override public int hashCode() {
      return Objects.hash(id, foo, bar);
    }
  }

  static class BusinessPayload {
    String id;
    String a;
    String b;

    public BusinessPayload(String id, String a, String b) {
      this.id = id;
      this.a = a;
      this.b = b;
    }

    @Override public String toString() {
      return "BusinessPayload{" + "id='" + id + '\'' +
          ", a='" + a + '\'' +
          ", b='" + b + '\'' +
          '}';
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BusinessPayload that = (BusinessPayload) o;
      return Objects.equals(id, that.id) &&
          Objects.equals(a, that.a) &&
          Objects.equals(b, that.b);
    }

    @Override public int hashCode() {
      return Objects.hash(id, a, b);
    }
  }
}
