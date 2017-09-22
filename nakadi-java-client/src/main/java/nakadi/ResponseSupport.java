package nakadi;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResponseSupport {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  public static void closeQuietly(Response res) {
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
        logger.warn(String.format("op=connection_close msg=error_clos_fail thread=%s %s %s",
            tName, res.hashCode(), res));
      }
    }
  }
}
