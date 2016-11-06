package nakadi;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

/**
 * Provide an {@link rx.Observable} that handles retries up to a configured number of attempts using
 * a retryWhenWithBackoff policy. If the backoff is exceeded, the  original observer's onError will
 * be invoked. The backoff can be configured to run up to {@link Integer#MAX_VALUE} times with an
 * arbitrary time delay and limited to a maximum delay time.
 */
class StreamConnectionRetry {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static int DEFAULT_INITIAL_DELAY_SECONDS = 1;
  static int DEFAULT_MAX_DELAY_SECONDS = 8;
  static int DEFAULT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  static TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
  private int attemptCount;

  /**
   * Allow an {@link rx.Observable} to be retried via {@link Observable#retryWhen} using a
   * backoff. If the backoff is exceeded, the original observer's onError will be invoked.
   *
   * @param maxAttempts how many times to drive a retry
   * @param initialDelay the starting delay
   * @param maxDelay the maximum delay that can be reached
   * @param unit the delay unit
   * @param scheduler the rx scheduler to run on
   * @param <T> the type we're composing with
   * @return an {@link Observable.Transformer} that can be given to  {@link Observable#compose}
   */
  <T> Observable.Transformer<T, T> retryWhenWithBackoff(int maxAttempts, int initialDelay,
      long maxDelay, final TimeUnit unit, Scheduler scheduler,
      Func1<Throwable, Boolean> isRetryable) {
    // todo: maxAttempts doesn't reset between successes; could replace with a supplier
    //noinspection Convert2Lambda
    return new Observable.Transformer<T, T>() {
      @Override
      public Observable<T> call(final Observable<T> observable) {
        return observable.retryWhen(
            eboRetry(maxAttempts, initialDelay, maxDelay, unit, isRetryable),
            scheduler
        );
      }
    };
  }

  private Func1<? super Observable<? extends Throwable>, ? extends Observable<?>>
  eboRetry(int maxAttempts, int initialDelay, long maxDelay, TimeUnit unit,
      Func1<Throwable, Boolean> isRetryable) {

    logger.info(
        String.format("Retry loading with, max_attempts=%s initial_delay=%s max_delay=%s time=%s",
            maxAttempts, initialDelay, maxDelay, unit.toString().toLowerCase()));

    //noinspection Convert2Lambda
    return new Func1<Observable<? extends Throwable>, Observable<?>>() {
      @SuppressWarnings("Convert2Lambda") @Override
      public Observable<?> call(Observable<? extends Throwable> observable) {

        return observable.
            zipWith(
                /*
                 zip() short circuits on the smallest observable of the two it's given; here those
                  two are the Observable<? extends Throwable> and Observable.range. because of this
                  zip won't fire if there is no error in the incoming observable. if there is an
                  error the range will emit its next "attempt" int value and the throwable/int will
                  be sent to the Func2. the Func2 wraps those in a narp and sends it onto flatMap.
                  the throwable is sent as that allows the flatMap to propagate it when the retry
                  gives up after the number of attempts. the propagation will end up in the client
                  supplied streamobserver's onError callback.
                  */
                Observable.range(1, maxAttempts),
                new Func2<Throwable, Integer, Narp>() {

                  @Override
                  public Narp call(Throwable throwable, Integer integer) {
                    return new Narp(integer, throwable);
                  }
                }
            )
            .flatMap(new Func1<Narp, Observable<?>>() {
              /*
              flatMap takes the Narp emitted by zip and computes a backoff. the backoff drives an
              Observable.timer which sleeps for a bit and drives a retry up into our main
              observable thanks to eboRetry being called from a retryWhen. If the max attempts are
              exceeded flatMap returns the throwable in an observer instead which breaks out of the
              retry loop and cascades up in the stream processor's onError. The same happens if the
              error is not considered retryable.
               */

              @Override
              public Observable<?> call(Narp narp) {

                attemptCount = narp.attempt;
                Throwable throwable = narp.throwable;

                if (!isRetryable.call(throwable)) {
                  logger.warn(
                      String.format(
                          "StreamConnectionRetry: not retryable, propagating error %s, %s",
                          throwable.getClass().getSimpleName(), throwable.getMessage()));

                  // This means onComplete will never be called from here because flatmap itself
                  // doesn't complete.
                  return Observable.error(throwable);
                }

                long delay = computeDelay(attemptCount, initialDelay, maxDelay, unit);

                if (attemptCount >= maxAttempts) {
                  logger.warn(
                      String.format(
                          "StreamConnectionRetry: cycle failed after %d attempts, propagating error %s, %s",
                          attemptCount, throwable.getClass().getSimpleName(),
                          throwable.getMessage()));

                  // This means onComplete will never be called from here because flatmap itself
                  // doesn't complete.
                  return Observable.error(throwable);
                } else {

                  logger.warn(
                      String.format(
                          "StreamConnectionRetry: will sleep for a bit, sleep=%s attempt=%d/%d error=%s",
                          delay, attemptCount,
                          maxAttempts, throwable.getMessage()));
                  return Observable.timer(delay, unit, Schedulers.computation()); // returns 0L
                }
              }
            });
      }
    };
  }

  @SuppressWarnings("WeakerAccess")
  @VisibleForTesting
  long computeDelay(long attemptCount, int initialDelay, long maxDelay, TimeUnit unit) {
    long delay = unit.toSeconds(initialDelay) * (attemptCount * attemptCount);

    if (delay < 0) {
      delay = unit.toSeconds(maxDelay);
    }

    if (delay > unit.toSeconds(maxDelay)) {
      delay = unit.toSeconds(maxDelay);
    }

    return delay;
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
