package nakadi;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import okhttp3.OkHttpClient;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventResourceTest {

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

  @Test
  public void sendWithScope() {

    final boolean[] askedForToken = {false};
    NakadiClient client = spy(NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .tokenProvider(scope -> {
          if(TokenProvider.NAKADI_EVENT_STREAM_WRITE.equals(scope)) {
            askedForToken[0] = true;
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
      new EventResource(client).send("foo", new Event<Happened>() {
        public Happened data() {
          return new Happened("a");
        }
      });
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
  }

  @Test
  public void serdesDomain() {
    EventResource eventResource = new EventResource(null);

    EventThing et = new EventThing("a", "b");
    EventRecord<EventThing> er = new EventRecord<>("topic", et);

    Object o = eventResource.mapEventRecordToSerdes(er);

    assertEquals(et, o);
  }

  @Test
  public void serdesUndefinedEventMapped() {
    EventResource eventResource = new EventResource(null);

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
    EventResource eventResource = new EventResource(null);

    BusinessEventMapped be = new BusinessEventMapped();
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
}