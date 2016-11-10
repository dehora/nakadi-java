package nakadi;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to log business events from a stream.
 */
public class LoggingBusinessEventObserver extends StreamObserverBackPressure<BusinessEventMapped> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  @Override public void onStart() {
    logger.info("LoggingBusinessEventObserver.onStart");
  }

  @Override public void onStop() {
    logger.info("LoggingBusinessEventObserver.onStop");
  }

  @Override public void onCompleted() {
    logger.info(String.format("LoggingBusinessEventObserver.onCompleted %s",
        Thread.currentThread().getName()));
  }

  @Override public void onError(Throwable e) {
    logger.info(String.format("LoggingBusinessEventObserver.onError %s %s", e.getMessage(),
        Thread.currentThread().getName()));
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void onNext(StreamBatchRecord<BusinessEventMapped> record) {
    final StreamOffsetObserver offsetObserver = record.streamOffsetObserver();
    final StreamBatch<BusinessEventMapped> batch = record.streamBatch();
    final StreamCursorContext cursor = record.streamCursorContext();

    logger.info(String.format("LoggingBusinessEventObserver: partition: %s ------------- %s",
        cursor.cursor().partition(), Thread.currentThread().getName()));

    if (batch.isEmpty()) {
      logger.info(String.format("LoggingBusinessEventObserver: partition: %s empty batch",
          cursor.cursor().partition()));
    } else {
      final List<BusinessEventMapped> events = batch.events();
      for (BusinessEventMapped event : events) {
        logger.info(String.format("LoggingBusinessEventObserver: EVENT: %s ", event));
      }
    }
    offsetObserver.onNext(record.streamCursorContext());
  }
}
