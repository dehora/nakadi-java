package nakadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

class StreamBatchRecordSubscriber<T> extends Subscriber<StreamBatchRecord<T>> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final StreamObserver<T> observer;
  private final MetricCollector metricCollector;

  StreamBatchRecordSubscriber(StreamObserver<T> observer, MetricCollector metricCollector) {
    this.observer = observer;
    this.metricCollector = metricCollector;
  }

  @Override public void onCompleted() {
    logger.info("StreamBatchRecordSubscriber.onCompleted");
    observer.onCompleted();
  }

  @Override public void onError(Throwable e) {
    e.printStackTrace();
    logger.info("StreamBatchRecordSubscriber.onError " + e.getMessage());
    observer.onError(e);
  }

  @Override public void onNext(StreamBatchRecord<T> record) {
    logger.debug("StreamBatchRecordSubscriber.onNext");
    if (!record.streamBatch().isEmpty()) {
      metricCollector.mark(MetricCollector.Meter.received, record.streamBatch().events().size());
    }
    observer.onNext(record);
    // allow the observer to set back pressure
    observer.requestBackPressure().ifPresent(this::request);
  }
}
