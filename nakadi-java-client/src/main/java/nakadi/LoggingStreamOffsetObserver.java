package nakadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingStreamOffsetObserver implements StreamOffsetObserver {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  @Override public void onNext(StreamCursorContext cursor) {
    logger.info(String.format("LoggingStreamOffsetObserver.onNext %s %s", cursor,
        Thread.currentThread().getName()));
  }
}
