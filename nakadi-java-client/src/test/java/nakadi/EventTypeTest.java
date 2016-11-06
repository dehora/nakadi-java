package nakadi;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class EventTypeTest {

  @Test
  public void testMarshal() {

    final String et = TestSupport.load("event-type-1.json");
    final NakadiClient client = TestSupport.newNakadiClient();
    final EventType eventType = client.jsonSupport().fromJson(et, EventType.class);
    //System.out.println(eventType);

    assertEquals("order.ORDER_RECEIVED", eventType.name());
    assertEquals("order-service", eventType.owningApplication());
    assertEquals(EventType.Category.business, eventType.category());
    assertEquals(Lists.newArrayList("metadata_enrichment"), eventType.enrichmentStrategies());
    assertEquals("random", eventType.partitionStrategy());
    assertNotNull(eventType.partitionKeyFields());
    assertNotNull(eventType.enrichmentStrategies());
    assertNull(eventType.eventTypeStatistics());
    assertNull(eventType.options());
  }

  @Test
  public void testRoundtrip() {

    final String et = TestSupport.load("event-type-1.json");
    final NakadiClient client = TestSupport.newNakadiClient();
    final EventType eventType1 = client.jsonSupport().fromJson(et, EventType.class);
    //System.out.println(et);

    final String et1 = client.jsonSupport().toJson(eventType1);
    final EventType eventType2 = client.jsonSupport().fromJson(et1, EventType.class);
    //System.out.println(et1);

    assertEquals("order.ORDER_RECEIVED", eventType2.name());
    assertEquals("order-service", eventType2.owningApplication());
    assertEquals(EventType.Category.business, eventType2.category());
    assertEquals(Lists.newArrayList("metadata_enrichment"), eventType2.enrichmentStrategies());
    assertEquals("random", eventType2.partitionStrategy());
    assertEquals(EventTypeSchema.Type.json_schema, eventType2.schema().type());
    assertEquals("{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }",
        eventType2.schema().schema());
    assertNotNull(eventType2.partitionKeyFields());
    assertNotNull(eventType2.enrichmentStrategies());
    assertNull(eventType2.eventTypeStatistics());
    assertNull(eventType2.options());
  }
}