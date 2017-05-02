package nakadi;

import com.google.gson.Gson;
import org.junit.Test;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EventMappedSupportTest {

  public static class Model {
    Integer id;
    Double num;
  }

  @Test
  public void numbers_gh119() {

    final NakadiClient client = TestSupport.newNakadiClient();

    final Model m = new Model();
    m.id = 100;
    m.num = 22.001;

    BusinessEventMapped<Model> change = new BusinessEventMapped<Model>()
        .metadata(new EventMetadata())
        .data(m);
    final Object mapped =
        EventMappedSupport.mapEventRecordToSerdes(new EventRecord<>("et1", change));
    final Gson gson = GsonSupport.gson();
    final String json = gson.toJson(mapped);

    // check our values aren't garbled; before fixing gh119, ints were being expanded to doubles

    assertFalse("expecting ints to not be converted to floats by gson", json.contains(": 100.0"));
    assertTrue("expecting ints to be present if set", json.contains(": 100"));
    assertTrue("expecting floats to be present if set", json.contains(": 22.001"));

    // check our overall structure is ok

    final BusinessEventMapped<Model> eventMapped = EventMappedSupport.marshalBusinessEventMapped(
            json,
            new TypeLiteral<BusinessEventMapped<Model>>() {
            }.type(),
            client.jsonSupport());

    // and check the values roundtrip ok

    assertSame(100, eventMapped.data().id);
    assertEquals(22.001, eventMapped.data().num, 0.0d);
  }
}