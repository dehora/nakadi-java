package nakadi.json.gson;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import nakadi.BusinessEventMapped;
import nakadi.DataChangeEvent;
import nakadi.EventMetadata;
import nakadi.EventRecord;
import nakadi.TypeLiteral;
import nakadi.UndefinedEventMapped;
import org.junit.Test;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GsonSupportTest {

  private final GsonSupport gsonSupport = new GsonSupport();

  @Test
  public void serdesWithGsonAnnotations() {

    final Serde s1 = new Serde("s1");
    final String s1json = gsonSupport.toJson(s1);
    assertTrue(s1json.contains("\"name\""));

    final Serde s2 = gsonSupport.fromJson(s1json, Serde.class);
    assertEquals("s1", s2.a);
  }

  @Test
  public void transformEventRecordUnwrapsUndefined() {

    final Serde e1 = new Serde("s1");
    final UndefinedEventMapped<Serde> u1 = new UndefinedEventMapped<>(e1);

    final EventRecord<UndefinedEventMapped<Serde>> et1 = new EventRecord<>("et1", u1);

    final Object o = gsonSupport.transformEventRecord(et1);
    assertTrue(o instanceof Serde);
    assertEquals("s1", ((Serde) o).a);

    final String raw = gsonSupport.toJson(o);

    final UndefinedEventMapped<Serde> e2 = gsonSupport.marshalUndefinedEventMapped(raw,
        new TypeLiteral<UndefinedEventMapped<Serde>>() {
        }.type());

    assertEquals("expect json field called 'name' to be gson marshaled to 'a'", "s1", e2.data().a);
  }

  @Test
  public void transformEventRecordBusinessEvent() {

    final Serde s1 = new Serde("s1");
    final EventMetadata em = new EventMetadata();
    final BusinessEventMapped<Serde> e1 = new BusinessEventMapped<>(s1, em);
    final EventRecord<BusinessEventMapped<Serde>> et1 = new EventRecord<>("et1", e1);

    final Object o = gsonSupport.transformEventRecord(et1);
    assertTrue("expect BusinessEventMapped transformed to JsonObject", o instanceof JsonObject);

    final String raw = gsonSupport.toJson(o);
    assertTrue("expect to see SerializedName value for field name", raw.contains("\"name\""));

    final BusinessEventMapped<Serde> e2 = gsonSupport.marshalBusinessEventMapped(raw,
        new TypeLiteral<BusinessEventMapped<Serde>>() {
        }.type());

    assertEquals("expect json field called 'name' to be gson marshaled to 'a'", "s1", e2.data().a);
  }

  @Test
  public void transformEventRecordDataEvent() {

    final Serde s1 = new Serde("s1");
    final EventMetadata em = new EventMetadata();
    final DataChangeEvent<Serde> e1 = new DataChangeEvent<Serde>()
        .metadata(em)
        .data(s1)
        .op(DataChangeEvent.Op.C)
        .dataType("et1");

    final EventRecord<DataChangeEvent<Serde>> et1 = new EventRecord<>("et1", e1);

    final Object o = gsonSupport.transformEventRecord(et1);
    assertTrue("expect DataChangeEvent untransformed", o instanceof DataChangeEvent);

    final String raw = gsonSupport.toJson(o);
    assertTrue("expect to see SerializedName value for field name", raw.contains("\"name\""));

    DataChangeEvent<Serde> e2 = gsonSupport.fromJson(raw, new TypeLiteral<DataChangeEvent<Serde>>() {
    }.type());

    assertEquals("expect json field called 'name' to be gson marshaled to 'a'", "s1", e2.data().a);
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

  public static class Serde {

    @SerializedName("name")
    String a;

    public Serde(String a) {
      this.a = a;
    }
  }

  public static class Model {

    Integer id;
    Double num;
  }
}