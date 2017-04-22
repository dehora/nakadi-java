package nakadi;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Function;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

/**
 * Handle retries up to a configured number of attempts using
 * a retryWhenWithBackoff policy. If the backoff is exceeded, the  original observer's onError will
 * be invoked. The backoff can be configured to run up to {@link Integer#MAX_VALUE} times with an
 * arbitrary time delay and limited to a maximum delay time.
 */
class StreamConnectionRetry {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static int DEFAULT_INITIAL_DELAY_SECONDS = 1;
  static int DEFAULT_MAX_DELAY_SECONDS = 8;
  static int DEFAULT_MIN_DELAY_SECONDS = 1;
  static int DEFAULT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  static TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
  private int attemptCount;

  /**
   * Allow a {@link Flowable} to be retried via {@link Flowable#retryWhen} using a
   * backoff. If the backoff is exceeded, the original observer's onError will be invoked.
   * <p>
   *   The last throw exception is tracked during retry. if the retry runs out of attempts
   *   as defined in the {@link RetryPolicy} the error is propagated back up to the client
   *   configured {@link nakadi.StreamObserver#onError} callback.
   * </p>
   *
   * @param backoff describes the backoff parameters
   * @param scheduler the rx scheduler to run on
   * @param <T> the type we're composing with ({@link FlowableTransformer}'s upstream and
   * downstream are the same type)
   * @return an {@link FlowableTransformer} that can be given to  {@link Flowable#compose}
   */
  <T> FlowableTransformer<T, T> retryWhenWithBackoff2(
      RetryPolicy backoff, io.reactivex.Scheduler scheduler, Function<Throwable, Boolean> isRetryable
  ) {
    return stream -> stream.observeOn(scheduler).retryWhen(
        flowable -> flowable.zipWith(
              /*
              zipWith short circuits on the smallest flowable of the two it's given. because
              of this zipWith won't fire if there is no error in the incoming "flowable".
              if there is an error the flowable.range will emit its next "attempt" int value
              and the throwable/int will be sent to the BiFunction in the 2nd argument.
              the BiFunction wraps the error and the count in a Narp which gets sends onwards
              to the flatMap. The throwable is sent as that allows the flatMap to propagate
              it when the retry gives up after the number of attempts. When propagation happens
              the throwable will end up in the client.
             */
            Flowable.range(1, backoff.maxAttempts()),
            (throwable, integer) -> new Narp(integer, throwable)
        ).flatMap(
             /*
              flatMap takes the Narp emitted by zipWith and computes a backoff. the backoff drives
              a Flowable.timer which sleeps for a bit and triggers a retry up into our main
              observable thanks to being being called from within a retryWhen. If the max attempts
              are exceeded flatMap returns the throwable in an observer instead which breaks out
              of the retry loop and cascades up in the StreamProcessor3's onError. The same happens
              if the error is not considered retryable (ie non-retryable exceptions fail fast).
              The (Function<Narp, Publisher<? extends Long>>) cast is there to help the compiler.
               */
            (Function<Narp, Publisher<? extends Long>>) narp -> {
              Throwable throwable = narp.throwable;
              if (!isRetryable.apply(throwable)) {
                logger.warn(String.format("StreamConnectionRetry not retryable, propagating %s, %s",
                        throwable.getClass().getSimpleName(), throwable.getMessage()));
                return Flowable.error(throwable);
              }

              if (backoff.isFinished()) {
                logger.warn(String.format(
                    "StreamConnectionRetry failed after %d attempts, propagating error %s, %s",
                    narp.attempt, throwable.getClass().getSimpleName(), throwable.getMessage()));
                return Flowable.error(throwable);
              } else {
                long delay = backoff.nextBackoffMillis();
                if (delay == RetryPolicy.STOP) {
                  logger.warn(String.format(
                      "StreamConnectionRetry being stopped agyer %d attempts, propagating error %s, %s",
                      narp.attempt, throwable.getClass().getSimpleName(), throwable.getMessage()));
                  return Flowable.error(throwable);
                }

                logger.info(String.format(
                    "connection_retry: will sleep for a bit, sleep=%s attempt=%d/%d error=%s",
                    delay, narp.attempt, backoff.maxAttempts(), throwable.getMessage()));

                return Flowable.timer(delay, TimeUnit.MILLISECONDS,
                    io.reactivex.schedulers.Schedulers.computation());
              }
            }
        )
    );
  }

  <T> Observable.Transformer<T, T> retryWhenWithBackoff(
      RetryPolicy backoff, Scheduler scheduler, Func1<Throwable, Boolean> isRetryable) {

    return observable -> observable.retryWhen(
        eboRetry(backoff, isRetryable),
        scheduler
    );
  }

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

    RetryPolicy backoff = ExponentialRetry.newBuilder()
        .initialInterval(initialDelay, unit)
        .maxInterval(maxDelay, unit)
        .maxAttempts(maxAttempts)
        .build();

    return retryWhenWithBackoff(backoff, scheduler, isRetryable);
  }

  private Func1<? super Observable<? extends Throwable>, ? extends Observable<?>>
  eboRetry(RetryPolicy backoff, Func1<Throwable, Boolean> isRetryable) {

    logger.info("Retry loading with, backoff={}", backoff);

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
                Observable.range(1, backoff.maxAttempts()),
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
                  // onComplete will never be called from here because flatmap doesn't complete.
                  return Observable.error(throwable);
                }

                if (backoff.isFinished()) {
                  logger.warn(
                      String.format(
                          "StreamConnectionRetry: cycle failed after %d attempts, propagating error %s, %s",
                          attemptCount, throwable.getClass().getSimpleName(),
                          throwable.getMessage()));
                  // onComplete will never be called from here because flatmap doesn't complete.
                  return Observable.error(throwable);
                } else {

                  long delay = backoff.nextBackoffMillis();

                  if(delay == RetryPolicy.STOP) {
                    logger.warn(
                        String.format(
                            "StreamConnectionRetry: cycle failed after %d attempts, propagating error %s, %s",
                            attemptCount, throwable.getClass().getSimpleName(),
                            throwable.getMessage()));
                    // onComplete will never be called from here because flatmap doesn't complete.
                    return Observable.error(throwable);
                  }

                  logger.info(
                      String.format(
                          "connection_retry: will sleep for a bit, sleep=%s attempt=%d/%d error=%s",
                          delay, attemptCount,
                          backoff.maxAttempts(), throwable.getMessage()));
                  return Observable.timer(delay, TimeUnit.MILLISECONDS, Schedulers.computation()); // returns 0L
                }
              }
            });
      }
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
