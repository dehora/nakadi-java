package nakadi;

import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to log {@link UndefinedEventMapped} events.
 */
public class LoggingUndefinedEventObserver
    extends StreamObserverBackPressure<UndefinedEventMapped> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  @Override public void onStart() {
    logger.info("LoggingUndefinedEventObserver.onStart");
  }

  @Override public void onStop() {
    logger.info("LoggingUndefinedEventObserver.onStop");
  }

  @Override public void onCompleted() {
    logger.info(String.format("LoggingUndefinedEventObserver.onCompleted %s",
        Thread.currentThread().getName()));
  }

  @Override public void onError(Throwable e) {
    logger.info(String.format("LoggingUndefinedEventObserver.onError %s %s", e.getMessage(),
        Thread.currentThread().getName()));
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void onNext(StreamBatchRecord<UndefinedEventMapped> record) {

    final StreamOffsetObserver offsetObserver = record.streamOffsetObserver();

    final StreamBatch<UndefinedEventMapped> batch = record.streamBatch();

    final StreamCursorContext cursor = record.streamCursorContext();

    logger.info(String.format("LoggingUndefinedEventObserver: partition: %s ------------- %s",
        cursor.cursor().partition(), Thread.currentThread().getName()));

    if (batch.isEmpty()) {
      logger.info(String.format("LoggingUndefinedEventObserver: partition: %s empty batch",
          cursor.cursor().partition()));
    } else {
      final List<UndefinedEventMapped> events = batch.events();
      for (UndefinedEventMapped event : events) {
        logger.info(String.format("LoggingUndefinedEventObserver: EVENT: %s ", event));
      }
    }

    offsetObserver.onNext(record.streamCursorContext());
  }

  @Override public Optional<Long> requestBackPressure() {
    return Optional.empty();
  }

  @Override public Optional<Integer> requestBuffer() {
    return Optional.empty();
  }
}
