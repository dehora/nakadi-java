package nakadi;

import com.google.gson.Gson;
import org.junit.Test;

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
}
