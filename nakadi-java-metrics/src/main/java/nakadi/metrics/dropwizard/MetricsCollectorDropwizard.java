package nakadi.metrics.dropwizard;

import com.codahale.metrics.MetricRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import nakadi.MetricCollector;
import nakadi.NakadiClient;
import nakadi.NakadiException;
import nakadi.VisibleForTesting;

/**
 * Routes metrics to a MetricRegistry. The collector can be supplied to the client via
 * {@link NakadiClient#metricCollector()}
 * <p/>
 * Metrics are prefixed with a supplied namespace. This allows multiple clients to use the same
 * underlying system used to capture the metrics. The metric name is emitted as dotted string, but
 * the event type's name uses hyphens to replace ' ', ':' and '.'.
 */
public class MetricsCollectorDropwizard implements MetricCollector {

  private final static Map<String, String> eventNames = new HashMap<>();

  private final String namespace;
  private final MetricRegistry metricRegistry;
  private final Map<Integer, com.codahale.metrics.Meter> meters = new HashMap<>();
  private final Map<Integer, com.codahale.metrics.Timer> timers = new HashMap<>();

  /**
   * Create a new MetricsCollector. Metrics will be prefixed with the non-optional supplied
   * namespace. This allows multiple clients to use the same underlying system used to capture
   * the metrics.
   *
   * @param namespace used to prefix metrics before emitted to the {@link MetricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} that will collect metrics.
   * @throws IllegalArgumentException if an argument is null.
   */
  public MetricsCollectorDropwizard(String namespace, MetricRegistry metricRegistry)
      throws IllegalArgumentException {
    NakadiException.throwNonNull(namespace, "Please provide a metric namespace");
    NakadiException.throwNonNull(metricRegistry, "Please provide a MetricRegistry");
    this.namespace = namespace;
    this.metricRegistry = metricRegistry;
  }

  @Override public void mark(MetricCollector.Meter meter) {
    mark(meter, 1);
  }

  @Override public void mark(MetricCollector.Meter event, long count) {
    findOrCreateMeter(event.path()).mark(count);
  }

  @Override public void duration(MetricCollector.Timer metric, long duration, TimeUnit unit) {
    findOrCreateTimer(metric.path()).update(duration, unit);
  }

  private com.codahale.metrics.Meter findOrCreateMeter(String metricPath) {
    final int hash = Objects.hash(namespace, metricPath);
    if (!meters.containsKey(hash)) {
      synchronized (meters) {
        meters.put(hash, metricRegistry.meter(name(namespace, metricPath)));
      }
    }
    return meters.get(hash);
  }

  private com.codahale.metrics.Timer findOrCreateTimer(String metricPath) {
    final int hash = Objects.hash(namespace, metricPath);
    if (!timers.containsKey(hash)) {
      synchronized (timers) {
        timers.put(hash, metricRegistry.timer(name(namespace, metricPath)));
      }
    }
    return timers.get(hash);
  }

  static String  name(String namespace, String metricName) {
    return MetricRegistry.name(namespace, metricName);
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  static String scrubEventTypeName(String eventType) {
    if(!eventNames.containsKey(eventType)) {
      eventNames.put(eventType, eventType.replaceAll("[.:\\s]", "-"));
    }

    return eventNames.get(eventType);
  }
}
