package nakadi;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Predicate;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide support for repeating up to a configured number of attempts
 * using a fixed delay between restarts. If the backoff is exceeded, the  original observer's
 * onError will be invoked. The restart can be configured to run up to {@link Integer#MAX_VALUE}
 * times with an arbitrary time delay and limited to a maximum number of restarts.
 */
class StreamConnectionRestart {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static int DEFAULT_DELAY_SECONDS = 3;
  static TimeUnit DEFAULT_DELAY_UNIT = TimeUnit.SECONDS;
  static int DEFAULT_MAX_RESTARTS = Integer.MAX_VALUE;

  StreamConnectionRestart() {
  }

  /**
   * Allow a {@link Flowable} to be restarted (repeated) via {@link Flowable#repeatWhen} using
   * a fixed delay for a configurable number of restarts. After the delay, the original observer
   * will be repeated unless the stopRepeatingPredicate function, which is called via {@link
   * Flowable#takeUntil}, short circuits the attempt.
   *
   * @param stopRestartingPredicate decide if the repeat should be applied or the observer we're
   * composing with should onComplete
   * @param restartDelay how long to wait between repeats
   * @param restartDelayUnit the time unit for repeats
   * @param maxRestarts the maximum number of repeats
   * @param <T> the type we're composing with ({@link FlowableTransformer}'s upstream and downstream
   * are the same type)
   * @return an {@link FlowableTransformer} that can be given to  {@link Flowable#compose}
   */
  <T> FlowableTransformer<T, T> repeatWhenWithDelayAndUntil(
      Predicate<Long> stopRestartingPredicate,
      long restartDelay,
      TimeUnit restartDelayUnit,
      int maxRestarts
  ) {
    return upstream -> upstream.repeatWhen(
        flowable -> flowable.zipWith(
            Flowable.range(1, maxRestarts),
            (obj, count) -> {
              logger.info("stream_repeater_requested {} restarts={}", obj == null ? "" : obj,
                  count);
              return count;
            }
        ).flatMap(
            attemptCount -> {
              logger.info("stream_repeater_delay  delay={} {}, restarts={}",
                  restartDelay, restartDelayUnit.toString().toLowerCase(), attemptCount);
              return Flowable.timer(restartDelay, restartDelayUnit);
            }
        ).takeUntil(
            stopRestartingPredicate
        ));
  }
}