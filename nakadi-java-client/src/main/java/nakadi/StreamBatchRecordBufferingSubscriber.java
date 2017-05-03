package nakadi;

import io.reactivex.subscribers.ResourceSubscriber;
import java.util.List;

class StreamBatchRecordBufferingSubscriber<T> extends
    ResourceSubscriber<List<StreamBatchRecord<T>>> {

  private final StreamObserver<T> observer;
  private MetricCollector metricCollector;

  StreamBatchRecordBufferingSubscriber(StreamObserver<T> observer,
      MetricCollector metricCollector) {
    super();
    this.observer = observer;
    this.metricCollector = metricCollector;
  }

  @Override protected void onStart() {
    super.onStart();
    observer.onStart();
  }

  @Override public void onComplete() {
    observer.onCompleted();
  }

  @Override public void onError(Throwable e) {
    observer.onError(e);
  }

  @Override public void onNext(List<StreamBatchRecord<T>> records) {
    records.forEach(record -> {
      if (!record.streamBatch().isEmpty()) {
        metricCollector.mark(MetricCollector.Meter.receivedBatch, 1);
        metricCollector.mark(MetricCollector.Meter.received, record.streamBatch().events().size());
      } else {
        metricCollector.mark(MetricCollector.Meter.receivedKeepalive, 1);
      }
      observer.onNext(record);
    });
    // allow the observer to set back pressure by requesting a number of items
    observer.requestBackPressure().ifPresent(this::request);
  }
}
