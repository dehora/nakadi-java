package nakadi;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StreamConnectionRetryFlowable implements
    Function<Flowable<? extends Throwable>, Publisher<Object>> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final RetryPolicy backoff;
  private final Function<Throwable, Boolean> isRetryable;

  StreamConnectionRetryFlowable(RetryPolicy backoff,
      Function<Throwable, Boolean> isRetryable) {
    this.backoff = backoff;
    this.isRetryable = isRetryable;
  }

  @Override public Publisher<Object> apply(Flowable<? extends Throwable> flowable)
      throws Exception {

    return flowable.flatMap(throwable -> {

      if (!isRetryable.apply(throwable)) {
        logger.warn(String.format("stream_retry not retryable, propagating %s, %s",
            throwable.getClass().getSimpleName(), throwable.getMessage()));

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
            "stream_retry: will sleep for a bit, sleep=%s attempt=%d/%d error=%s",
            delay, backoff.workingAttempts(), backoff.maxAttempts(), throwable.getMessage()));

        return Flowable.timer(delay, MILLISECONDS);
      }
    });
  }
}