package nakadi;

import io.reactivex.subscribers.ResourceSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamBatchRecordSubscriber<T> extends ResourceSubscriber<StreamBatchRecord<T>> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final StreamObserver<T> observer;
  private final MetricCollector metricCollector;

  StreamBatchRecordSubscriber(StreamObserver<T> observer, MetricCollector metricCollector) {
    super();
    this.observer = observer;
    this.metricCollector = metricCollector;
  }

  @Override protected void onStart() {
    super.onStart();
    observer.onStart();
  }

  @Override public void onComplete() {
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
      metricCollector.mark(MetricCollector.Meter.receivedBatch, 1);
      metricCollector.mark(MetricCollector.Meter.received, record.streamBatch().events().size());
    } else {
      metricCollector.mark(MetricCollector.Meter.receivedKeepalive, 1);
    }
    observer.onNext(record);
    // allow the observer to set back pressure by requesting a number of items
    observer.requestBackPressure().ifPresent(this::request);
  }


}
