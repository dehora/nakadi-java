package nakadi.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import nakadi.MetricCollector;
import nakadi.NakadiClient;
import nakadi.NakadiException;

/**
 * Routes metrics to a micrometer MeterRegistry. The collector can be supplied to the client via
 * {@link NakadiClient#metricCollector()}
 * <p>
 * Metrics are prefixed with a supplied namespace. This allows multiple clients to use the same
 * underlying system used to capture the metrics.
 *
 * The class is thread-safe
 * </p>
 */
public class MetricsCollectorMicrometer implements MetricCollector {

  private final MeterRegistry meterRegistry;
  private final String namespace;

  /**
   * Create a new MetricsCollector. Metrics will be prefixed with the non-optional supplied
   * namespace. This allows multiple clients to use the same underlying system used to capture
   * the metrics.
   *
   * @param namespace used to prefix metrics before emitted to the {@link MeterRegistry}.
   * @param meterRegistry the {@link MeterRegistry} that will collect metrics.
   * @throws IllegalArgumentException if an argument is null.
   */
  public MetricsCollectorMicrometer(String namespace, MeterRegistry meterRegistry)
      throws IllegalArgumentException {
    NakadiException.throwNonNull(namespace, "Please provide a metric namespace");
    NakadiException.throwNonNull(meterRegistry, "Please provide a MeterRegistry");
    this.meterRegistry = meterRegistry;
    this.namespace = namespace;
  }

  @Override
  public void mark(Meter meter) {
    meterRegistry.counter(name(meter.path())).increment();
  }

  @Override
  public void mark(Meter meter, long count) {
    meterRegistry.counter(name(meter.path())).increment(count);
  }

  @Override
  public void duration(Timer timer, long duration, TimeUnit unit) {
    meterRegistry.timer(name(timer.path())).record(duration, unit);
  }

  private String name(String path) {
    return String.join(".", this.namespace, path);
  }
}
