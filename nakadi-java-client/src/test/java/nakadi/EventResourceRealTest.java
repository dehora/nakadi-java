package nakadi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.util.Collections;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventResourceRealTest {

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
      server.start(InetAddress.getByName("localhost"), 8312);
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
  public void undefinedIsSentToServerMapped() throws Exception {

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:8312")
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
        .baseURI("http://localhost:8312")
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
  public void sendWithScope() {

    final boolean[] askedForToken = {false};
    final boolean[] askedForCustomScope = {false};
    String customScope = "custom";

    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .tokenProvider(scope -> {
          if (TokenProvider.NAKADI_EVENT_STREAM_WRITE.equals(scope)) {
            askedForToken[0] = true;
          }

          if (customScope.equals(scope)) {
            askedForCustomScope[0] = true;
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
      assertFalse(askedForToken[0]);
      assertFalse(askedForCustomScope[0]);

      new EventResourceReal(client)
          .send("foo", Lists.newArrayList(new Event<Happened>() {
        public Happened data() {
          return new Happened("a");
        }
      }));
    } catch(NetworkException | NotFoundException ignored) {
    }

    ArgumentCaptor<ResourceOptions> options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/event-types/foo/events"),
        options.capture(),
        Matchers.anyList());

    assertEquals(TokenProvider.NAKADI_EVENT_STREAM_WRITE, options.getValue().scope());
    assertTrue(askedForToken[0]);
    assertFalse(askedForCustomScope[0]);


    r = spy(new OkHttpResource(
        new OkHttpClient.Builder().build(),
        new GsonSupport(),
        mock(MetricCollector.class)));

    when(client.resourceProvider()).thenReturn(mock(ResourceProvider.class));
    when(client.resourceProvider().newResource()).thenReturn(r);

    try {
      askedForToken[0] = false;
      askedForCustomScope[0] = false;
      assertFalse(askedForToken[0]);
      assertFalse(askedForCustomScope[0]);

      new EventResourceReal(client)
          .scope(customScope)
          .send("foo", Lists.newArrayList(new Event<Happened>() {
        public Happened data() {
          return new Happened("a");
        }
      }));
    } catch(NetworkException | NotFoundException ignored) {
    }

    options = ArgumentCaptor.forClass(ResourceOptions.class);

    verify(r, times(1)).requestThrowing(
        Matchers.eq(Resource.POST),
        Matchers.eq("http://localhost:9080/event-types/foo/events"),
        options.capture(),
        Matchers.anyList());

    assertEquals(customScope, options.getValue().scope());
    assertTrue(askedForCustomScope[0]);
    assertFalse(askedForToken[0]);

  }

  @Test
  public void serdesDomain() {
    EventResourceReal eventResource = new EventResourceReal(null);

    EventThing et = new EventThing("a", "b");
    EventRecord<EventThing> er = new EventRecord<>("topic", et);

    Object o = eventResource.mapEventRecordToSerdes(er);

    assertEquals(et, o);
  }

  @Test
  public void serdesUndefinedEventMapped() {
    EventResourceReal eventResource = new EventResourceReal(null);

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
    EventResourceReal eventResource = new EventResourceReal(null);

    BusinessEventMapped<Map<String, Object>> be = new BusinessEventMapped<>();
    EventMetadata em = new EventMetadata();
    em.eid("eid1");
    Map<String, Object> uemap = Maps.newHashMap();
    uemap.put("a", "1");
    be.data(uemap);
    be.metadata(em);

    EventRecord<BusinessEventMapped> er = new EventRecord<>("topic", be);
    Map<String, Object> outmap = (Map<String, Object>) eventResource.mapEventRecordToSerdes(er);
    assertTrue(outmap.size() == 2);
    assertTrue(outmap.containsKey("metadata"));
    assertTrue(outmap.containsKey("a"));
    assertEquals("1", outmap.get("a"));

    assertEquals(em, outmap.get("metadata"));
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