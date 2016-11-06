package nakadi;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventResourceTest {

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

    UndefinedEventMapped ue = new UndefinedEventMapped();
    Map<String, Object> uemap = Maps.newHashMap();
    uemap.put("a", "1");
    ue.data(uemap);
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