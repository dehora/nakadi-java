package nakadi;

import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.functions.Function;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle retries up to a configured number of attempts using a retryWhen policy.
 * If the backoff is exceeded, the  original observer's onError will be invoked. The backoff can be
 * configured to run up to {@link Integer#MAX_VALUE} times with an arbitrary time delay and limited
 * to a maximum delay time.
 */
class RequestRetry {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  <T> ObservableTransformer<T, T> retryWhenWithBackoffObserver(
      RetryPolicy backoff,
      io.reactivex.Scheduler scheduler,
      Function<Throwable, Boolean> isRetryable
  ) {
    return o -> {
      logger.info("request_retry loading with, backoff={}", backoff);
      return o.observeOn(scheduler).retryWhen(observable -> observable.
          zipWith(
              Observable.range(1, backoff.maxAttempts()),
              (throwable, integer) -> new Narp(integer, throwable)
          )
          .flatMap((Function<Narp, Observable<?>>) narp -> {
            final Throwable throwable = narp.throwable;
            if (!isRetryable.apply(throwable)) {
              logger.warn(String.format(
                  "request_retry: not retryable, propagating error %s, %s",
                  throwable.getClass().getSimpleName(), throwable.getMessage()));
              // onComplete will never be called from here because flatmap doesn't complete.
              return Observable.error(throwable);
            }

            if (backoff.isFinished()) {
              logger.warn(String.format(
                  "request_retry: cycle failed after %d attempts, propagating error %s, %s",
                  narp.attempt, throwable.getClass().getSimpleName(), throwable.getMessage()));
              // onComplete will never be called from here because flatmap doesn't complete.
              return Observable.error(throwable);
            } else {
              long delay = backoff.nextBackoffMillis();
              if (delay == RetryPolicy.STOP) {
                logger.warn(String.format(
                    "request_retry: cycle failed after %d attempts, propagating error %s, %s",
                    narp.attempt, throwable.getClass().getSimpleName(), throwable.getMessage()));
                // onComplete will never be called from here because flatmap doesn't complete.
                return Observable.error(throwable);
              }

              logger.info(String.format(
                  "request_retry: will sleep for a bit, sleep=%s attempt=%d/%d error=%s",
                  delay, narp.attempt, backoff.maxAttempts(), throwable.getMessage()));
              return Observable.timer(delay, TimeUnit.MILLISECONDS, scheduler); // returns 0L
            }
          }));
    };
  }

  private static class Narp {

    Integer attempt;
    Throwable throwable;

    Narp(Integer attempt, Throwable throwable) {
      this.attempt = attempt;
      this.throwable = throwable;
    }
  }
}