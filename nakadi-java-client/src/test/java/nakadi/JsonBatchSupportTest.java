package nakadi;

import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonBatchSupportTest {

  private final GsonSupport jsonSupport = new GsonSupport();
  private final JsonBatchSupport support = new JsonBatchSupport(jsonSupport);
  private final StreamOffsetObserver observer = e -> {
  };
  private final String undefinedLine = TestSupport.load("undefined-event-batch-1.json");
  private final String businessLine = TestSupport.load("business-event-batch-1.json");
  private final String dataLine = TestSupport.load("data-change-event-batch-1.json");
  private final String spanCtxDataLine = TestSupport.load("data-change-event-batch-span-ctx.json");
  private final String spanCtxBusinessLine = TestSupport.load("business-event-batch-span-ctx.json");


  @Test
  public void testBusinessEventCanMarshalASpan() {

    TypeLiteral<BusinessEventMapped<ID>> eventType =
        new TypeLiteral<BusinessEventMapped<ID>>() {
        };

    StreamBatchRecord<BusinessEventMapped<ID>> sbr =
        support.lineToEventStreamBatchRecord(spanCtxBusinessLine, eventType.type(),
            new LoggingStreamOffsetObserver());

    BusinessEventMapped<ID> event = sbr.streamBatch().events().get(0);

    final Map<String, String> spanCtx = event.metadata().spanCtx();

    assertEquals(spanCtx.get("ot-tracer-spanid"), "span_01cvssvfcm346zn4nn6c7xmc49");
    assertEquals(spanCtx.get("ot-tracer-traceid"), "trc_01cvsskn8542fh1zmxc6018vb4");
    assertEquals(spanCtx.get("ot-baggage-one"), "two");
    assertEquals(spanCtx.get("ot-tag-three"), "four");
    assertEquals(spanCtx.get("ot-childof"), "span_01cvsszwkkees9ag0szyg9t4tw");
  }

  @Test
  public void testDataChangeEventCanMarshalASpan() {

    TypeLiteral<DataChangeEvent<ID>> eventType =
        new TypeLiteral<DataChangeEvent<ID>>() {
        };

    StreamBatchRecord<DataChangeEvent<ID>> sbr =
        support.lineToEventStreamBatchRecord(spanCtxDataLine, eventType.type(),
            new LoggingStreamOffsetObserver());

    DataChangeEvent<ID> event = sbr.streamBatch().events().get(0);

    final Map<String, String> spanCtx = event.metadata().spanCtx();

    assertEquals(spanCtx.get("ot-tracer-spanid"), "span_01ghst6jv93yj9p8r9ezrh33m1");
    assertEquals(spanCtx.get("ot-tracer-traceid"), "trc_01cvst6jv93yj9p8r9ezrh3em0");
    assertEquals(spanCtx.get("ot-baggage-foo"), "bar");
  }

  @Test
  public void testUndefinedMarshalNotParameterized() {

    // the type literal is fine, but the UndefinedEventMapped has no parameter
    TypeLiteral<UndefinedEventMapped> literal = new TypeLiteral<UndefinedEventMapped>() {};

    try {
      StreamBatchRecord<UndefinedEventMapped> sbr =
          support.lineToEventStreamBatchRecord(undefinedLine, literal.type(), observer);
      fail("An un-parameterized UndefinedEventMapped type literal should not be processed");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void testUndefinedCanMarshalAGeneric() {

    TypeLiteral<UndefinedEventMapped<UndefinedPayload>> eventType =
        new TypeLiteral<UndefinedEventMapped<UndefinedPayload>>() {
        };

    StreamBatchRecord<UndefinedEventMapped<UndefinedPayload>> sbr =
        support.lineToEventStreamBatchRecord(undefinedLine, eventType.type(), observer);

    assertEquals("36", sbr.streamBatch().cursor().offset());
    assertEquals("0", sbr.streamBatch().cursor().partition());

    UndefinedEventMapped<UndefinedPayload> event = sbr.streamBatch().events().get(0);
    UndefinedPayload data = event.data();
    assertEquals("1", data.id);
    assertEquals("2", data.foo);
    assertEquals("3", data.bar);
  }

  @Test
  public void testUndefinedCanMarshalAGenericMap() {

    TypeLiteral<UndefinedEventMapped<Map<String, Object>>> eventType =
        new TypeLiteral<UndefinedEventMapped<Map<String, Object>>>() {
        };

    StreamBatchRecord<UndefinedEventMapped<Map<String, Object>>> sbr =
        support.lineToEventStreamBatchRecord(undefinedLine, eventType.type(), observer);

    assertEquals("36", sbr.streamBatch().cursor().offset());
    assertEquals("0", sbr.streamBatch().cursor().partition());

    Map<String, Object> expected = Maps.newHashMap();
    expected.put("id", "1");
    expected.put("foo", "2");
    expected.put("bar", "3");

    UndefinedEventMapped<Map<String, Object>> event = sbr.streamBatch().events().get(0);
    assertEquals(expected, event.data());
  }

  @Test
  public void testBusinessMarshalRejectsNotParameterized() {

    // the type literal is fine, but the BusinessEventMapped has no parameter
    TypeLiteral<BusinessEventMapped> literal = new TypeLiteral<BusinessEventMapped>() {};

    try {
      StreamBatchRecord<BusinessEventMapped> sbr =
          support.lineToEventStreamBatchRecord(businessLine, literal.type(), observer);
      fail("An un-parameterized BusinessEventMapped type literal should not be processed");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void testBusinessCanMarshalAGeneric() {


    TypeLiteral<BusinessEventMapped<UndefinedPayload>> eventType =
        new TypeLiteral<BusinessEventMapped<UndefinedPayload>>() {
        };

    StreamBatchRecord<BusinessEventMapped<UndefinedPayload>> sbr =
        support.lineToEventStreamBatchRecord(businessLine, eventType.type(), observer);

    BusinessEventMapped<UndefinedPayload> event = sbr.streamBatch().events().get(0);

    assertEquals("36", sbr.streamBatch().cursor().offset());
    assertEquals("0", sbr.streamBatch().cursor().partition());

    EventMetadata metadata = event.metadata();
    assertEquals("2016-09-20T21:52Z", metadata.occurredAt().toString());
    assertEquals("a2ab0b7c-ee58-48e5-b96a-d13bce73d857", metadata.eid());
    assertEquals("et-1", metadata.eventType());
    assertEquals("0", metadata.partition());
    assertEquals("2016-10-26T18:12:20.712Z", metadata.receivedAt().toString());
    assertEquals("Nt0oU70k3UCNp2NKugrIF0QU", metadata.flowId());
    assertEquals("a-compaction-key", metadata.partitionCompactionKey());

    UndefinedPayload data = event.data();
    assertEquals("1", data.id);
    assertEquals("2", data.foo);
    assertEquals("3", data.bar);
  }

  @Test
  public void testBusinessCanMarshalAGenericMap() {

    TypeLiteral<BusinessEventMapped<Map<String, Object>>> eventType =
        new TypeLiteral<BusinessEventMapped<Map<String, Object>>>() {
        };

    StreamBatchRecord<BusinessEventMapped<Map<String, Object>>> sbr =
        support.lineToEventStreamBatchRecord(businessLine, eventType.type(), observer);

    BusinessEventMapped<Map<String, Object>> event = sbr.streamBatch().events().get(0);

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
  public void testDataChangeEventCanMarshalAGeneric() {

    TypeLiteral<DataChangeEvent<ID>> eventType =
        new TypeLiteral<DataChangeEvent<ID>>() {
        };

    StreamBatchRecord<DataChangeEvent<ID>> sbr =
        support.lineToEventStreamBatchRecord(dataLine, eventType.type(),
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
  public void testTypeLiteralCanMarshalAnEmptyBatch() {
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

  private static class UndefinedPayload {
    String id;
    String foo;
    String bar;


    UndefinedPayload(String id, String foo, String bar) {
      this.id = id;
      this.foo = foo;
      this.bar = bar;
    }

  }

  private static class ID {
    String id;
  }

}
