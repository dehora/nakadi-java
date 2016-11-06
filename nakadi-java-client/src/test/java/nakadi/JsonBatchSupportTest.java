package nakadi;

import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonBatchSupportTest {

  private final GsonSupport jsonSupport = new GsonSupport();
  private final JsonBatchSupport support = new JsonBatchSupport(jsonSupport);

  @Test
  public void testUndefinedMarshal() {

    final String line = TestSupport.load("undefined-event-batch-1.json");

    TypeLiteral<UndefinedEventMapped> eventType =
        new TypeLiteral<UndefinedEventMapped>() {
        };

    StreamBatchRecord<UndefinedEventMapped> sbr =
        support.lineToEventStreamBatchRecord(line, eventType.type(), e -> {
        });

    assertEquals("36", sbr.streamBatch().cursor().offset());
    assertEquals("0", sbr.streamBatch().cursor().partition());

    Map<String, Object> expected = Maps.newHashMap();
    expected.put("id", "1");
    expected.put("foo", "2");
    expected.put("bar", "3");

    UndefinedEventMapped event = sbr.streamBatch().events().get(0);
    assertEquals(expected, event.data());
  }

  @Test
  public void testBusinessMarshal() {

    final String line = TestSupport.load("business-event-batch-1.json");

    TypeLiteral<BusinessEventMapped> eventType =
        new TypeLiteral<BusinessEventMapped>() {
        };

    StreamBatchRecord<BusinessEventMapped> sbr =
        support.lineToEventStreamBatchRecord(line, eventType.type(), e -> {
        });

    BusinessEventMapped event = sbr.streamBatch().events().get(0);

    assertEquals("36", sbr.streamBatch().cursor().offset());
    assertEquals("0", sbr.streamBatch().cursor().partition());

    EventMetadata metadata = event.metadata();
    assertEquals("2016-09-20T21:52Z", metadata.occurredAt().toString());
    assertEquals("a2ab0b7c-ee58-48e5-b96a-d13bce73d857", metadata.eid());
    assertEquals("et-1", metadata.eventType());
    assertEquals("0", metadata.partition());
    assertEquals("2016-10-26T18:12:20.712Z", metadata.receivedAt().toString());
    assertEquals("Nt0oU70k3UCNp2NKugrIF0QU", metadata.flowId());

    Map<String, Object> expected = Maps.newHashMap();
    expected.put("id", "1");
    expected.put("foo", "2");
    expected.put("bar", "3");

    assertEquals(expected, event.data());
  }

  @Test
  public void testTypeLiteralMarshal() {

    final String line = TestSupport.load("data-change-event-batch-1.json");

    TypeLiteral<DataChangeEvent<ID>> eventType =
        new TypeLiteral<DataChangeEvent<ID>>() {
        };

    StreamBatchRecord<DataChangeEvent<ID>> sbr =
        support.lineToEventStreamBatchRecord(line, eventType.type(),
            new LoggingStreamOffsetObserver());

    assertEquals("95", sbr.streamBatch().cursor().offset());
    assertEquals("0", sbr.streamBatch().cursor().partition());

    DataChangeEvent<ID> event = sbr.streamBatch().events().get(0);
    // these two are redundant but express intent
    assertEquals(event.getClass(), DataChangeEvent.class);
    assertEquals(event.data().getClass(), ID.class);

    assertEquals("1", event.data().id);
    assertEquals(DataChangeEvent.Op.C, event.op());
    assertEquals("a2ab0b7c-ee58-48e5-b96a-d13bce73d857", event.metadata().eid());
    assertEquals("2016-09-20T21:52Z", event.metadata().occurredAt().toString());
  }

  @Test
  public void testTypeLiteralMarshalEmptyBatch() {
    TypeLiteral<DataChangeEvent<ID>> eventType =
        new TypeLiteral<DataChangeEvent<ID>>() {
        };

    String empty = "{\"cursor\":{\"partition\":\"0\",\"offset\":\"95\"}}";
    StreamBatchRecord<Object> sbr1 =
        support.lineToEventStreamBatchRecord(empty, eventType.type(),
            new LoggingStreamOffsetObserver());

    assertEquals("95", sbr1.streamBatch().cursor().offset());
    assertEquals("0", sbr1.streamBatch().cursor().partition());
    assertTrue(sbr1.streamBatch().events().size() == 0);
  }

  private static class ID {
    String id;
  }
}