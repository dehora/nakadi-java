package nakadi;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamExceptionSupport {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static boolean isSubscriptionRetryable(Throwable e) {
    if (e instanceof ConflictException) {
      if (logger.isDebugEnabled()) {
        logger.debug("Retryable exception for subscription: " + e.getMessage(), e);
      } else {
        logger.info("Retryable exception for subscription: " + e.getMessage());
      }
      return true;
    }

    return StreamExceptionSupport.isRetryable(e);
  }

  @SuppressWarnings("WeakerAccess")
  @VisibleForTesting
  static boolean isRetryable(Throwable e) {

    if (e instanceof UncheckedIOException || e instanceof EOFException) {
      if (e.getCause() instanceof java.net.SocketTimeoutException) {
        logger.warn("Retryable socket timeout exception: " + e.getMessage(), e);
        return true;
      }

      /*
       can indicate the server connection went away abruptly or it's not up currently. a normal
       stream end and close from the server won't cause this
        */
      logger.warn("Retryable io/eof exception: " + e.getMessage(), e);
      return true;
    }

    if (e instanceof NakadiException) {
      NakadiException n = (NakadiException) e;

      if (n instanceof NetworkException) {
        logger.warn("Retryable network exception: " + n.getMessage());
        return true;
      }

      if (n instanceof ServerException) {
        logger.warn("Retryable server exception: " + n.getMessage());
        return true;
      }

      if (n instanceof RateLimitException) {
        logger.warn("Rate limit exception: " + n.getMessage());
        return true; // todo: ideally we handle this differently
      }
    }

    if (e instanceof IOException) {
      logger.warn(String.format("Non-retryable  io exception %s", e.getMessage()));
      return false; // todo: investigate if this can be retryable
    }

    logger.warn(
        String.format("Non-retryable exception: %s %s", e.getClass(), e.getMessage()));

    return false; // likelihood is we'll be coming back here for a while qualifying errors.
  }
}
