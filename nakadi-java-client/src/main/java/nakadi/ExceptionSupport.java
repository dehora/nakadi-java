package nakadi;

import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExceptionSupport {

  private static final ImmutableMap<Integer, Class> CODES_TO_EXCEPTIONS =
      ImmutableMap.<Integer, Class>builder()
          .put(400, ClientException.class)
          .put(401, AuthorizationException.class)
          .put(403, AuthorizationException.class)
          .put(404, NotFoundException.class)
          .put(409, ConflictException.class)
          .put(412, PreconditionFailedException.class)
          .put(422, InvalidException.class)
          .put(429, RateLimitException.class)
          .put(500, ServerException.class)
          .put(503, ServerException.class)
          .build();

  static Map<Integer, Class> responseCodesToExceptionsMap() {
    return CODES_TO_EXCEPTIONS;
  }

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static boolean isSubscriptionStreamRetryable(Throwable e) {
    if (e instanceof ConflictException) {
      if (logger.isDebugEnabled()) {
        logger.debug("Retryable exception for subscription: " + e.getMessage(), e);
      } else {
        logger.info("Retryable exception for subscription: " + e.getMessage());
      }
      return true;
    }

    return ExceptionSupport.isEventStreamRetryable(e);
  }

  @SuppressWarnings("WeakerAccess")
  @VisibleForTesting
  static boolean isEventStreamRetryable(Throwable e) {

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
        return true;
      }
    }

    if (e instanceof IOException) {
      logger.warn(String.format("Non-retryable  io exception %s", e.getMessage()));
      return false; // todo: investigate if this can be retryable
    }

    if (e instanceof java.util.concurrent.TimeoutException) {
      logger.warn(
          "Retryable timeout exception, maybe due to the server not sending keepalives in time {}",
          e.getMessage());
      return true;
    }

    logger.warn(
        String.format("Non-retryable exception: %s %s", e.getClass(), e.getMessage()));

    return false; // likelihood is we'll be coming back here for a while qualifying errors.
  }
}
