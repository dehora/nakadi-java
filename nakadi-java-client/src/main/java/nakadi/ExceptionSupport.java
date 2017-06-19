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
  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

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

  static boolean isConsumerStreamRetryable(Throwable e) {
    if (e instanceof Error) {
      logger.error(String.format("non_retryable_error_class_consumer %s %s",
          e.getClass(), e.getMessage()), e);
      return false;
    }

    if (e instanceof NonRetryableNakadiException) {
      logger.error(String.format("non_retryable_nakadi_exception_consumer %s %s",
          e.getClass(), e.getMessage()), e);
      return false;
    }

    logger.info(String.format("retryable_exception %s %s", e.getClass(), e.getMessage()), e);
    return true;
  }

  @SuppressWarnings("WeakerAccess")
  @VisibleForTesting
  static boolean isApiRequestRetryable(Throwable e) {

    if (e instanceof Error) {
      logger.error(String.format("non_retryable_error_class_api %s %s",
          e.getClass(), e.getMessage()), e);
      return false;
    }

    if (e instanceof UncheckedIOException || e instanceof EOFException) {
      if (e.getCause() instanceof java.net.SocketTimeoutException) {
        logger.warn("retryable_socket_timeout_exception err=" + e.getMessage(), e);
        return true;
      }

      /*
       can indicate the server connection went away abruptly or it's not up currently.
        */
      logger.warn("retryable_io_eof_exception err=" + e.getMessage(), e);
      return true;
    }

    if (e instanceof NakadiException) {
      NakadiException n = (NakadiException) e;

      if (n instanceof NetworkException) {
        logger.warn("retryable_network_exception {} ", e.getCause());
        return true;
      }

      if (n instanceof ServerException) {
        logger.warn("retryable_server_exception err=" + n.getMessage());
        return true;
      }

      if (n instanceof RateLimitException) {
        logger.warn("rate_limit_exception err=" + n.getMessage());
        return true;
      }

      if (n instanceof AuthorizationException) {
        logger.warn("retryable_auth_exception err=" + n.getMessage());
        return true;
      }

      if (n instanceof ContractRetryableException) {
        logger.warn("retryable_contract_exception err=" + n.getMessage());
        return true;
      }

      if (n instanceof RetryableException) {
        logger.warn("retryable_exception err=" + n.getMessage());
        return true;
      }
    }

    if (e instanceof IOException) {
      logger.warn(String.format("retryable_io_exception err=%s", e.getMessage()), e);
      return true;
    }

    if (e instanceof java.util.concurrent.TimeoutException) {
      logger.warn("retryable_concurrent_timeout_exception err={}", e.getMessage());
      return true;
    }

    logger.warn(
        String.format("non_retryable_exception %s %s", e.getClass(), e.getMessage()), e);

    return false; // likelihood is we'll be coming back here for a while qualifying errors.
  }
}
