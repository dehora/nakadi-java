package nakadi;

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
    logger.debug("StreamBatchRecordSubscriber.onStart");
    observer.onBegin();
  }

  @Override public void onNext(StreamBatchRecord<T> record) {

    if (done) {
      return;
    }

    if (record == null) {
      NullPointerException npe = new NullPointerException("onNext called with null batch record. "
          + "Null values are not expected from stream processors.");
      onError(npe);
      throw npe;
    }

    try {
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
      logger.warn("StreamBatchRecordSubscriber.retryable_exception msg=" + e.getMessage(), e);
    } catch (NonRetryableNakadiException e) {
      logger.warn("StreamBatchRecordSubscriber.non_retryable_exception msg=" + e.getMessage());
      onError(e);
      throw e;
    } catch (Throwable t) {
      if (t instanceof Error) {
        logger.error("StreamBatchRecordSubscriber.detected_error msg={}", t.getMessage());
        onError(t);
        throw (Error) t;
      }

      if (!ExceptionSupport.isConsumerStreamRetryable(t)) {
        logger.error(String.format(
            "StreamBatchRecordSubscriber.detected_nonretryable_exception type=%s msg=%s", t.getClass().getSimpleName(),
            t.getMessage()));

        onError(t);
        throw t;
      } else {
        logger.info(String.format(
            "StreamBatchRecordSubscriber.detected_retryable_exception type=%s msg=%s", t.getClass().getSimpleName(),
            t.getMessage()));
      }
    }
  }

  @Override public void onError(Throwable e) {
    logger.error("StreamBatchRecordSubscriber.onError " + e.getMessage());

    if (done) {
      logger.warn("observer_on_error_exception msg=onError_already_called");
      return;
    }

    done = true;

    try {
      observer.onError(e);
    } catch (Exception e1) {
      throw new NonRetryableNakadiException(
          Problem.localProblem("observer_on_error_exception", "observer.onError_threw_exception"),
          e1);
    }
  }

  @Override public void onComplete() {
    logger.info("StreamBatchRecordSubscriber.onCompleted");
    observer.onCompleted();
  }
}
