package nakadi;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StreamConnectionRetryFlowable implements
    Function<Flowable<? extends Throwable>, Publisher<Object>> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  static int DEFAULT_INITIAL_DELAY_SECONDS = 1;
  static int DEFAULT_MAX_DELAY_SECONDS = 8;
  static int DEFAULT_MIN_DELAY_SECONDS = 1;
  static int DEFAULT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  static TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

  private final RetryPolicy backoff;
  private final Function<Throwable, Boolean> isRetryable;
  private MetricCollector metricCollector;

  StreamConnectionRetryFlowable(RetryPolicy backoff,
      Function<Throwable, Boolean> isRetryable,
      MetricCollector metricCollector
  ) {
    this.backoff = backoff;
    this.isRetryable = isRetryable;
    this.metricCollector = metricCollector;
  }

  @Override public Publisher<Object> apply(Flowable<? extends Throwable> flowable)
      throws Exception {

    return flowable.flatMap(throwable -> {

      if (!isRetryable.apply(throwable)) {
        logger.warn(String.format("stream_retry_not_retryable thread=%s propagating %s, %s",
            Thread.currentThread().getName(), throwable.getClass().getSimpleName(),
            throwable.getMessage()));

        return Flowable.error(throwable);
      }

      if (backoff.isFinished()) {
        logger.warn(String.format(
            "stream_retry failed after %d attempts, propagating error %s, %s",
            backoff.workingAttempts(), throwable.getClass().getSimpleName(),
            throwable.getMessage()));
        return Flowable.error(throwable);
      } else {
        final long delay = backoff.nextBackoffMillis();
        if (delay == RetryPolicy.STOP) {
          logger.warn(String.format(
              "stream_retry being stopped after %d attempts, propagating error %s, %s",
              backoff.workingAttempts(), throwable.getClass().getSimpleName(),
              throwable.getMessage()));
          return Flowable.error(throwable);
        }

        logger.info(String.format(
            "stream_retry_will_sleep sleep=%s attempt=%d/%d thread=%s error=%s",
            delay, backoff.workingAttempts(), backoff.maxAttempts(),
            Thread.currentThread().getName(), throwable.getMessage()));

        metricCollector.mark(MetricCollector.Meter.consumerRetry);

        return Flowable.timer(delay, MILLISECONDS);
      }
    });
  }
}
