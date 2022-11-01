package nakadi;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GsonSupportTest {

  @Test
  public void flat() {

    final GsonSupport gsonSupport = new GsonSupport();
    final Model m = new Model();
    m.id = 1;
    m.num = 2.0D;
    assertFalse(gsonSupport.toJsonCompressed(m).contains(" "));
    assertTrue(gsonSupport.toJson(m).contains(" "));
  }

  @Test
  public void string() {

    final GsonSupport gsonSupport = new GsonSupport();

    TypeLiteral<String> typeLiteral = new TypeLiteral<String>() {
    };

    String json =
        "{\"metadata\":{\"occurred_at\":\"2016-09-20T21:52:00Z\",\"eid\":\"a2ab0b7c-ee58-48e5-b96a-d13bce73d857\",\"event_type\":\"et-1\",\"partition\":\"0\",\"received_at\":\"2016-10-26T20:54:41.300Z\",\"flow_id\":\"nP1I7tHXBwICOh5HqLnICOPh\"},\"data_op\":\"C\",\"data\":{\"id\":\"1\"},\"data_type\":\"et-1\"}";

    gsonSupport.fromJson(json, String.class);
    gsonSupport.fromJson(json, typeLiteral.type());
  }

  public static class Model {

    Integer id;
    Double num;
  }

  @Test
  public void numbers_gh119() {

    final GsonSupport gsonSupport = new GsonSupport();
    final Model m = new Model();
    m.id = 100;
    m.num = 22.001;

    BusinessEventMapped<Model> change = new BusinessEventMapped<Model>()
        .metadata(new EventMetadata())
        .data(m);
    final Object mapped = gsonSupport.transformEventRecord(new EventRecord<>("et1", change));
    final Gson gson = GsonSupport.gson();
    final String json = gson.toJson(mapped);

    // check our values aren't garbled; before fixing gh119, ints were being expanded to doubles

    assertFalse("expecting ints to not be converted to floats by gson", json.contains(": 100.0"));
    assertTrue("expecting ints to be present if set", json.contains(": 100"));
    assertTrue("expecting floats to be present if set", json.contains(": 22.001"));

    // check our overall structure is ok

    final BusinessEventMapped<Model> eventMapped =
        gsonSupport.marshalBusinessEventMapped(
            json,
            new TypeLiteral<BusinessEventMapped<Model>>() {
            }.type());

    // and check the values roundtrip ok

    assertSame(100, eventMapped.data().id);
    assertEquals(22.001, eventMapped.data().num, 0.0d);
  }

  @Test
  public void minifyObject() {
    final GsonSupport gsonSupport = new GsonSupport();

    final Model m = new Model();
    m.id = 100;
    m.num = 22.001;

    String minified = gsonSupport.toJsonCompressed(m);
    assertTrue(minified.equals("{\"id\":100,\"num\":22.001}"));
  }

  @Test
  public void minifyString() {
    final GsonSupport gsonSupport = new GsonSupport();

    String raw = TestSupport.load("data-change-event-single-compress.json");
    String minified = gsonSupport.toJsonCompressed(raw);

    // gson doesn't minify raw strings it escapes them; note the result starts with " not {
    assertTrue(minified.startsWith("\"{"));

    // to minify cleanly we need to read the string from raw and minify the object representation
    minified = gsonSupport.toJsonCompressed(gsonSupport.fromJson(raw, Object.class));
    assertTrue(minified.startsWith("{\"m"));
  }

  @Test
  public void serdesDomain() {
    JsonSupport jsonSupport = new GsonSupport();

    EventThing et = new EventThing("a", "b");
    EventRecord<EventThing> er = new EventRecord<>("topic", et);

    Object o = jsonSupport.transformEventRecord(er);
    assertEquals(et, o);
  }

  @Test
  public void serdesUndefinedEventMapped() {
    JsonSupport jsonSupport = new GsonSupport();

    Map<String, Object> uemap = Maps.newHashMap();
    uemap.put("a", "1");
    UndefinedEventMapped<Map<String, Object>> ue =
            new UndefinedEventMapped<Map<String, Object>>().data(uemap);
    EventRecord<UndefinedEventMapped> er = new EventRecord<>("topic", ue);
    Map<String, Object> outmap = (Map<String, Object>) jsonSupport.transformEventRecord(er);
    Assert.assertTrue(outmap.size() == 1);
    Assert.assertTrue(outmap.containsKey("a"));
    assertEquals("1", outmap.get("a"));
  }

  @Test
  public void serdesBusinessEventMapped() {
    JsonSupport jsonSupport = new GsonSupport();

    BusinessEventMapped<Map<String, Object>> be = new BusinessEventMapped<>();
    EventMetadata em = new EventMetadata();
    em.eid("eid1").withFlowId().withOccurredAt();
    Map<String, Object> uemap = Maps.newHashMap();
    uemap.put("a", 1);
    uemap.put("b", 22.0001);
    uemap.put("c", "40.0001");
    uemap.put("d", "c22.0001");
    be.data(uemap);
    be.metadata(em);

    EventRecord<BusinessEventMapped> er = new EventRecord<>("topic", be);
    JsonObject outmap = (JsonObject) jsonSupport.transformEventRecord(er);
    Assert.assertTrue(outmap.size() == 5);
    Assert.assertTrue(outmap.get("metadata") != null);
    Assert.assertTrue(outmap.get("a") != null);
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

}
