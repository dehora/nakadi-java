package nakadi;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.functions.Function;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle retries up to a configured number of attempts using a retryWhenWithBackoffObserver policy.
 * If the backoff is exceeded, the  original observer's onError will be invoked. The backoff can be
 * configured to run up to {@link Integer#MAX_VALUE} times with an arbitrary time delay and limited
 * to a maximum delay time.
 */
@Deprecated
/*
todo: fix for rxjaav2 or remove
Since rxjava2 this is failing to release each event; it buffers them up until the sub checkpointer
times out which causes a disconnect that invalidates the session id for the released event set.
 The StreamConnectionRetryFlowable class is a workaround until this is debugged properly
 */
class StreamConnectionRetry {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static int DEFAULT_INITIAL_DELAY_SECONDS = 1;
  static int DEFAULT_MAX_DELAY_SECONDS = 8;
  static int DEFAULT_MIN_DELAY_SECONDS = 1;
  static int DEFAULT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  static TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

  /**
   * Allow a {@link Flowable} to be retried via {@link Flowable#retryWhen} using a
   * backoff. If the backoff is exceeded, the original observer's onError will be invoked.
   * <p>
   * The last throw exception is tracked during retry. if the retry runs out of attempts
   * as defined in the {@link RetryPolicy} the error is propagated back up to the client
   * configured {@link nakadi.StreamObserver#onError} callback.
   * </p>
   *
   * @param backoff describes the backoff parameters
   * @param scheduler the rx scheduler to run on
   * @param <T> the type we're composing with ({@link FlowableTransformer}'s upstream and downstream
   * are the same type)
   * @return an {@link FlowableTransformer} that can be given to  {@link Flowable#compose}
   */
  @Deprecated
  <T> FlowableTransformer<T, T> retryWhenWithBackoffTransformer(
      RetryPolicy backoff,
      io.reactivex.Scheduler scheduler,
      Function<Throwable, Boolean> isRetryable
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
                logger.warn(String.format("connection_retry not retryable, propagating %s, %s",
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
                      "connection_retry being stopped after %d attempts, propagating error %s, %s",
                      narp.attempt, throwable.getClass().getSimpleName(), throwable.getMessage()));
                  return Flowable.error(throwable);
                }

                logger.info(String.format(
                    "connection_retry: will sleep for a bit, sleep=%s attempt=%d/%d error=%s",
                    delay, narp.attempt, backoff.maxAttempts(), throwable.getMessage()));

                return Flowable.timer(delay, TimeUnit.MILLISECONDS);
              }
            }
        )
    );
  }

  <T> ObservableTransformer<T, T> retryWhenWithBackoffObserver(
      RetryPolicy backoff,
      io.reactivex.Scheduler scheduler,
      Function<Throwable, Boolean> isRetryable
  ) {
    return o -> {
      logger.info("Retry loading with, backoff={}", backoff);
      return o.observeOn(scheduler).retryWhen(observable -> observable.
          zipWith(
              Observable.range(1, backoff.maxAttempts()),
              (throwable, integer) -> new Narp(integer, throwable)
          )
          .flatMap((Function<Narp, Observable<?>>) narp -> {
            final Throwable throwable = narp.throwable;
            if (!isRetryable.apply(throwable)) {
              logger.warn(String.format(
                  "connection_retry: not retryable, propagating error %s, %s",
                  throwable.getClass().getSimpleName(), throwable.getMessage()));
              // onComplete will never be called from here because flatmap doesn't complete.
              return Observable.error(throwable);
            }

            if (backoff.isFinished()) {
              logger.warn(String.format(
                  "connection_retry: cycle failed after %d attempts, propagating error %s, %s",
                  narp.attempt, throwable.getClass().getSimpleName(), throwable.getMessage()));
              // onComplete will never be called from here because flatmap doesn't complete.
              return Observable.error(throwable);
            } else {
              long delay = backoff.nextBackoffMillis();
              if (delay == RetryPolicy.STOP) {
                logger.warn(String.format(
                    "connection_retry: cycle failed after %d attempts, propagating error %s, %s",
                    narp.attempt, throwable.getClass().getSimpleName(), throwable.getMessage()));
                // onComplete will never be called from here because flatmap doesn't complete.
                return Observable.error(throwable);
              }

              logger.info(String.format(
                  "connection_retry: will sleep for a bit, sleep=%s attempt=%d/%d error=%s",
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