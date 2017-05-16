package nakadi;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExceptionSupport {

  private static final Map<Integer, Class> CODES_TO_EXCEPTIONS = new HashMap<>();
  static {
    CODES_TO_EXCEPTIONS.put(400, ClientException.class);
    CODES_TO_EXCEPTIONS.put(401, AuthorizationException.class);
    CODES_TO_EXCEPTIONS.put(403, AuthorizationException.class);
    CODES_TO_EXCEPTIONS.put(404, NotFoundException.class);
    CODES_TO_EXCEPTIONS.put(409, ConflictException.class);
    CODES_TO_EXCEPTIONS.put(412, PreconditionFailedException.class);
    CODES_TO_EXCEPTIONS.put(422, InvalidException.class);
    CODES_TO_EXCEPTIONS.put(429, RateLimitException.class);
    CODES_TO_EXCEPTIONS.put(500, ServerException.class);
    CODES_TO_EXCEPTIONS.put(503, ServerException.class);
  }

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

      if (n instanceof AuthorizationException) {
        logger.warn("retryable_auth_error exception: " + n.getMessage());
        return true;
      }

      if (n instanceof ContractRetryableException) {
        logger.warn("retryable_contract_error exception: " + n.getMessage());
        return true;
      }
    }

    if (e instanceof IOException) {
      logger.warn(String.format("Non-retryable io exception %s", e.getMessage()), e);
      return false; // todo: investigate if this can be retryable
    }

    if (e instanceof java.util.concurrent.TimeoutException) {
      logger.warn(
          "Retryable timeout exception, maybe due to the server not sending keepalives in time {}",
          e.getMessage());
      return true;
    }

    logger.warn(
        String.format("Non-retryable exception: %s %s", e.getClass(), e.getMessage()), e);

    return false; // likelihood is we'll be coming back here for a while qualifying errors.
  }
}
