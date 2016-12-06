package nakadi;

import java.util.List;
import rx.Subscriber;

class StreamBatchRecordBufferingSubscriber<T> extends Subscriber<List<StreamBatchRecord<T>>> {

  private final StreamObserver<T> observer;
  private MetricCollector metricCollector;

  StreamBatchRecordBufferingSubscriber(StreamObserver<T> observer,
      MetricCollector metricCollector) {
    this.observer = observer;
    this.metricCollector = metricCollector;
  }

  @Override public void onCompleted() {
    observer.onCompleted();
  }

  @Override public void onError(Throwable e) {
    observer.onError(e);
  }

  @Override public void onNext(List<StreamBatchRecord<T>> records) {
    records.forEach(record -> {
      if (!record.streamBatch().isEmpty()) {
        metricCollector.mark(MetricCollector.Meter.received, record.streamBatch().events().size());
      }
      observer.onNext(record);
    });
    // allow the observer to set back pressure
    // todo: revisit this for high volume streams
    observer.requestBackPressure().ifPresent(this::request);
  }
}
