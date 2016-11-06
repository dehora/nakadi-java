package nakadi;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricCollectorSafely implements MetricCollector {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final MetricCollector metricCollector;

  public MetricCollectorSafely(MetricCollector metricCollector) {
    this.metricCollector = metricCollector;
  }

  @Override public void mark(Meter meter) {

    try {
      metricCollector.mark(meter);
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  @Override public void mark(Meter meter, long count) {
    try {
      metricCollector.mark(meter, count);
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  @Override public void duration(Timer timer, long duration, TimeUnit unit) {
    try {
      metricCollector.duration(timer, duration, unit);
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }
}
