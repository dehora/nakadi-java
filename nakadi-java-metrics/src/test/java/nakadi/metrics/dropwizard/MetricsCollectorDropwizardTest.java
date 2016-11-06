package nakadi.metrics.dropwizard;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import nakadi.MetricCollector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricsCollectorDropwizardTest {

  private MetricRegistry metricRegistry = new MetricRegistry();

  @Test
  public void metricNames() {

    assertEquals("event-type-thing",
        MetricsCollectorDropwizard.scrubEventTypeName("event:type:thing"));

    assertEquals("event-type-thing",
        MetricsCollectorDropwizard.scrubEventTypeName("event type thing"));

    assertEquals("event-type-thing",
        MetricsCollectorDropwizard.scrubEventTypeName("event.type.thing"));

    assertEquals("event--type--thing--is",
        MetricsCollectorDropwizard.scrubEventTypeName("event::type..thing  is"));
  }

  @Test
  public void metrics() {
    MetricsCollectorDropwizard metrics = new MetricsCollectorDropwizard("woo", metricRegistry);

    metrics.duration(MetricCollector.Timer.eventSend, 10000,
        TimeUnit.NANOSECONDS);

    assertEquals(1, metricRegistry.getTimers().entrySet().size());
    Map<String, Timer> timers = metricRegistry.getTimers();

    String nameEventSendTime =
        MetricsCollectorDropwizard.name("woo", MetricCollector.Timer.eventSend.path());

    assertTrue(timers.containsKey(nameEventSendTime));

    assertTrue(10000.0 == timers.get(nameEventSendTime).getSnapshot().getMean());

    metrics.mark(MetricCollector.Meter.sent);
    metrics.mark(MetricCollector.Meter.sent, 100);

    Map<String, Meter> meters = metricRegistry.getMeters();
    Set<Map.Entry<String, Meter>> entries = meters.entrySet();
    assertEquals(1, entries.size());

    String nameEventSent =
        MetricsCollectorDropwizard.name("woo", MetricCollector.Meter.sent.path());

    assertTrue(meters.containsKey(nameEventSent));
    assertEquals(101, meters.get(nameEventSent).getCount());

    metrics.mark(MetricCollector.Meter.http409);
    meters = metricRegistry.getMeters();
    entries = meters.entrySet();
    assertEquals(2, entries.size());

    String name409 =
        MetricsCollectorDropwizard.name("woo", MetricCollector.Meter.http409.path());

    assertTrue(meters.containsKey(name409));
    assertTrue(1 == meters.get(name409).getCount());
  }
}