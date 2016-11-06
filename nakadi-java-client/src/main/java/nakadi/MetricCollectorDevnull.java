package nakadi;

import java.util.concurrent.TimeUnit;

class MetricCollectorDevnull implements MetricCollector {

  @Override public void mark(Meter meter) {

  }

  @Override public void mark(Meter event, long count) {

  }

  @Override public void duration(Timer metric, long duration, TimeUnit unit) {

  }
}
