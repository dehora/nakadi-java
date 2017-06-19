package nakadi;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamProcessorHalfOpenKick {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final int DEFAULT_HALF_OPEN_CONNECTION_GRACE_SECONDS = 90;

  private final TimeUnit halfOpenUnit = TimeUnit.SECONDS;
  private final long halfOpenKick;

  StreamProcessorHalfOpenKick(final long batchFlushTimeoutSeconds) {
    final long halfOpenGrace = DEFAULT_HALF_OPEN_CONNECTION_GRACE_SECONDS;
    halfOpenKick = halfOpenUnit.toSeconds(batchFlushTimeoutSeconds + halfOpenGrace);
    logger.info(
        "configuring half open timeout, batch_flush_timeout={}, grace_period={}, disconnect_after={} {}",
        batchFlushTimeoutSeconds, halfOpenGrace, halfOpenKick, halfOpenUnit.name().toLowerCase());
  }

  TimeUnit halfOpenUnit() {
    return halfOpenUnit;
  }

  long halfOpenKick() {
    return halfOpenKick;
  }
}
