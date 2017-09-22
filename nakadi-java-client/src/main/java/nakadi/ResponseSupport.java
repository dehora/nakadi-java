package nakadi;

import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResponseSupport {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static void closeQuietly(Response res) {
    final String tName = Thread.currentThread().getName();
    boolean closed = false;

    try {
      logger.debug("op=connection_close msg=close_ask thread={} res_hash={} res={}",
          tName, res.hashCode(), res);
      res.responseBody().close();
      closed = true;
    } catch (Exception e) {
      logger.error("op=connection_close msg=err_close thread={} class={} err={} res_hash={} res={}",
          tName, e.getClass().getName(), e.getMessage(), res.hashCode(), res);
    } finally {
      if (!closed) {
        try {
          res.responseBody().close();
          closed = true;
        } catch (IOException e) {
          logger.error(
              "op=connection_close msg=err_close_reattempt thread={}  class={} err={} res_hash={} res={}",
              tName, e.getClass().getName(), e.getMessage(), res.hashCode(), res);
        }
      }

      if (!closed) {
        logger.warn(String.format("op=connection_close msg=error_close_fail thread=%s %s %s",
            tName, res.hashCode(), res));
      }
    }
  }

  static void closeQuietly(Closeable closeable, int attempts) {
    final String tName = Thread.currentThread().getName();
    boolean closed = false;
    try {
      logger.debug("response_close_ask thread={}", tName);
      closeable.close();
      logger.debug("response_close_ok thread={}", tName);
      closed = true;
    } catch (Exception e) {
      logger.error("response_close_error problem closing on {} {}", e.getClass().getName(),
          e.getMessage());
    } finally {
      int attempt = 0;
      while (!closed && attempt++ < attempts) {
        try {
          closeable.close();
          closed = true;
        } catch (Exception e1) {
          logger.error("response_close_error retrying close attempts {}/{} on {}", attempt,
              attempts, e1.getMessage());
        }
      }

      if (!closed) {
        logger.error("response_close_error could not close http response attempts {}/{}", attempt,
            attempts);
      }
    }
  }
}
