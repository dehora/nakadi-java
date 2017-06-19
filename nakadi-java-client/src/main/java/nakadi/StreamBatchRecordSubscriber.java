package nakadi;

import io.reactivex.exceptions.CompositeException;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.subscribers.ResourceSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamBatchRecordSubscriber<T> extends ResourceSubscriber<StreamBatchRecord<T>> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final StreamObserver<T> observer;
  private final MetricCollector metricCollector;
  private boolean done;

  StreamBatchRecordSubscriber(StreamObserver<T> observer, MetricCollector metricCollector) {
    super();
    this.observer = observer;
    this.metricCollector = metricCollector;
  }

  @Override protected void onStart() {
    super.onStart();
    observer.onStart();
  }

  @Override public void onNext(StreamBatchRecord<T> record) {

    if (done) {
      return;
    }

    if (record == null) {
      Throwable npe = new NullPointerException("onNext called with null batch record. "
          + "Null values are not expected from stream processors.");

      try {
        dispose();
      } catch (Throwable t1) {
        throwOnFatal(t1);
        logger.error(t1.getMessage(), t1);
        onError(npe);
        return;
      }
      onError(npe);
      return;
    }

    try {
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
    } catch (RetryableException e) {
      /*
       the observer's telling us to keep going
        */
      logger.warn("observer_retryable_exception msg=" + e.getMessage(), e);
    } catch (Throwable t) {
      logger.warn("observer_non_retryable_exception msg=" + t.getMessage(), t);

      try {
        dispose();
      } catch (Throwable t1) {
        throwOnFatal(t1);
        logger.error(t1.getMessage(), t1);
        onError(t);
        return;
      }

      onError(t);

      throwOnFatal(t);
    }
  }

  @Override public void onError(Throwable e) {
    logger.info("StreamBatchRecordSubscriber.onError " + e.getMessage());

    if (done) {
      RxJavaPlugins.onError(e);
      return;
    }

    done = true;

    try {
      observer.onError(e);
    } catch (Exception e1) {
      throwOnFatal(e1);
      logger.error(e1.getMessage(), e1);
      RxJavaPlugins.onError(new CompositeException(e, e1));
    }
  }

  @Override public void onComplete() {
    logger.info("StreamBatchRecordSubscriber.onCompleted");
    observer.onCompleted();
  }

  private void throwOnFatal(Throwable t) {
    Exceptions.throwIfFatal(t);
    if (t instanceof Error) {
      throw (Error) t;
    }
  }
}
