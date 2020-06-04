package nakadi.metrics.micrometer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import nakadi.MetricCollector;
import org.junit.Test;

public class MetricsCollectorMicrometerTest {

  @Test
  public void metrics() {
    String namespace = "foo";
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsCollectorMicrometer metrics = new MetricsCollectorMicrometer(namespace, meterRegistry);

    // duration
    metrics.duration(MetricCollector.Timer.eventSend, 1, TimeUnit.SECONDS);
    assertEquals(1, meterRegistry.getMeters().size());

    String eventSendTimeName = name(namespace, MetricCollector.Timer.eventSend.path());

    assertTrue(matchValue(meterRegistry, eventSendTimeName, 1));

    // mark
    metrics.mark(MetricCollector.Meter.sent);
    assertEquals(2, meterRegistry.getMeters().size());

    String eventSendName = name(namespace, MetricCollector.Meter.sent.path());

    assertTrue(matchValue(meterRegistry, eventSendName, 1));

    // mark with value
    metrics.mark(MetricCollector.Meter.http409, 50);
    assertEquals(3, meterRegistry.getMeters().size());

    String name409 = name(namespace, MetricCollector.Meter.http409.path());

    assertTrue(matchValue(meterRegistry, name409,50));
  }

  private String name(String namespace, String path) {
    return String.join(".", namespace, path);
  }

  private boolean matchValue(MeterRegistry meterRegistry, String meterName, double value) {
    return meterRegistry.getMeters().stream()
        .filter(meter -> meter.getId().getName().equals(meterName))
        .map(Meter::measure)
        .flatMap(measurements -> StreamSupport.stream(measurements.spliterator(), false))
        .limit(1)
        .anyMatch(measurement -> measurement.getValue() == value);
  }
}
