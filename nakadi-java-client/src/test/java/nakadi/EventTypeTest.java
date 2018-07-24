package nakadi;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class EventTypeTest {

  @Test
  public void testEventTypeAuthorizationSerialization() {
    EventType et = new EventType();

    EventTypeAuthorization authorization0 = new EventTypeAuthorization()
        .admin(new AuthorizationAttribute().dataType(AuthorizationAttribute.WILDCARD).value("a"))
        .reader(new AuthorizationAttribute().dataType(AuthorizationAttribute.WILDCARD).value("r"))
        .writer(new AuthorizationAttribute().dataType(AuthorizationAttribute.WILDCARD).value("w"));

    et.authorization(authorization0);
    final NakadiClient client = TestSupport.newNakadiClient();
    final String s = client.jsonSupport().toJson(et);
    final EventType eventType = client.jsonSupport().fromJson(s, EventType.class);

    final EventTypeAuthorization authorization = eventType.authorization();
    assertNotNull(authorization);
    assertNotNull(authorization.admins());
    assertEquals(1, authorization.admins().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.admins().get(0).dataType());
    assertEquals("a", authorization.admins().get(0).value());
    assertNotNull(authorization.readers());
    assertEquals(1, authorization.readers().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.readers().get(0).dataType());
    assertEquals("r", authorization.readers().get(0).value());
    assertNotNull(authorization.writers());
    assertEquals(1, authorization.writers().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.writers().get(0).dataType());
    assertEquals("w", authorization.writers().get(0).value());
  }

  @Test
  public void testEventTypeStatisticsSerialization() {

    EventType et = new EventType();
    EventTypeStatistics ets = new EventTypeStatistics(1,2,3,4);

    et.eventTypeStatistics(ets);

    final NakadiClient client = TestSupport.newNakadiClient();
    final String s = client.jsonSupport().toJson(et);
    final EventType eventType = client.jsonSupport().fromJson(s, EventType.class);

    assertEquals(1, eventType.eventTypeStatistics().messagesPerMinute());
    assertEquals(2, eventType.eventTypeStatistics().messageSize());
    assertEquals(3, eventType.eventTypeStatistics().readParallelism());
    assertEquals(4, eventType.eventTypeStatistics().writeParallelism());
  }

  @Test
  public void testFields() {

    try {
      EventType et = new EventType();
      et.name("ff");
      et.category(EventType.Category.business);
      et.enrichmentStrategies("dd");
      et.enrichmentStrategy("dd");
      et.owningApplication("dd");
      et.partitionStrategy("dd");
      et.partitionKeyFields("dd");
      et.schema(new EventTypeSchema());
    } catch (Exception e) {
      fail("broken field check " +e.getMessage());
    }
  }

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
    assertEquals("delete", eventType.cleanupPolicy());
    assertNotNull(eventType.partitionKeyFields());
    assertNotNull(eventType.enrichmentStrategies());
    assertNull(eventType.eventTypeStatistics());
    assertNull(eventType.options());
    final EventTypeAuthorization authorization = eventType.authorization();
    assertNotNull(authorization);
    assertNotNull(authorization.admins());
    assertEquals(1, authorization.admins().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.admins().get(0).dataType());
    assertEquals("a", authorization.admins().get(0).value());
    assertNotNull(authorization.readers());
    assertEquals(1, authorization.readers().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.readers().get(0).dataType());
    assertEquals("r", authorization.readers().get(0).value());
    assertNotNull(authorization.writers());
    assertEquals(1, authorization.writers().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.writers().get(0).dataType());
    assertEquals("w", authorization.writers().get(0).value());

  }

  @Test
  public void testRoundtrip() {

    final String et = TestSupport.load("event-type-1.json");
    final NakadiClient client = TestSupport.newNakadiClient();
    final EventType eventType1 = client.jsonSupport().fromJson(et, EventType.class);
    //System.out.println(et);

    final String et1 = client.jsonSupport().toJson(eventType1);
    final EventType eventType2 = client.jsonSupport().fromJson(et1, EventType.class);
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
    final EventTypeAuthorization authorization = eventType2.authorization();
    assertNotNull(authorization);
    assertNotNull(authorization.admins());
    assertEquals(1, authorization.admins().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.admins().get(0).dataType());
    assertEquals("a", authorization.admins().get(0).value());
    assertNotNull(authorization.readers());
    assertEquals(1, authorization.readers().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.readers().get(0).dataType());
    assertEquals("r", authorization.readers().get(0).value());
    assertNotNull(authorization.writers());
    assertEquals(1, authorization.writers().size());
    assertEquals(AuthorizationAttribute.WILDCARD, authorization.writers().get(0).dataType());
    assertEquals("w", authorization.writers().get(0).value());
  }
}
