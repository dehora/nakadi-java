package nakadi;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

public class EventMetadataTest {

  @Test
  public void testSpan() {
    final EventMetadata eventMetadata = EventMetadata.newPreparedEventMetadata();
    assertNull(eventMetadata.spanCtx());
    final Map<String, String> ctx = Maps.newHashMap();
    ctx.put("ot-tracer-spanid", "42");
    ctx.put("ot-tracer-traceid", "e9435c17dabe8238");
    ctx.put("ot-baggage-foo", "bar");

    eventMetadata.spanCtx(ctx);
    assertEquals(ctx, eventMetadata.spanCtx());
  }

  @Test
  public void testNewPreparedEventMetadata() {
    final EventMetadata eventMetadata = EventMetadata.newPreparedEventMetadata();
    assertNotNull(eventMetadata.eid());
    assertNotNull(eventMetadata.occurredAt());
    assertNotNull(eventMetadata.flowId());
  }

  @Test
  public void testNewEventMetadata() {
    /*
     https://github.com/dehora/nakadi-java/issues/321
     default values for these three, especially eid had a performance overhead
      */
    final EventMetadata eventMetadata = new EventMetadata();
    assertNull(eventMetadata.eid());
    assertNull(eventMetadata.occurredAt());
    assertNull(eventMetadata.flowId());
  }

  @Test
  public void testNewEventMetadataCompaction() {
    final EventMetadata eventMetadata = new EventMetadata();
    assertNull(eventMetadata.partitionCompactionKey());
    eventMetadata.partitionCompactionKey("123");
    assertEquals("123", eventMetadata.partitionCompactionKey());

    final EventMetadata preparedEventMetadata = EventMetadata.newPreparedEventMetadata();
    assertNull(preparedEventMetadata.partitionCompactionKey());
    preparedEventMetadata.partitionCompactionKey("1234");
    assertEquals("1234", preparedEventMetadata.partitionCompactionKey());
  }

}
