package nakadi;

import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Helper class to log events from a stream.
 */
public class LoggingStreamObserver extends StreamObserverBackPressure<String> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  @Override public void onStart() {
    logger.info("LoggingStreamObserver.onStart");
  }

  @Override public void onStop() {
    logger.info("LoggingStreamObserver.onStop");
  }

  @Override public void onCompleted() {
    logger.info("LoggingStreamObserver.onCompleted");
  }

  @Override public void onError(Throwable e) {
    logger.warn("LoggingStreamObserver.onError {}", e.getMessage());
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void onNext(StreamBatchRecord<String> record) {

    try {
      final StreamOffsetObserver offsetObserver = record.streamOffsetObserver();
      final StreamBatch<String> batch = record.streamBatch();
      final StreamCursorContext context = record.streamCursorContext();

      MDC.put("cursor_context", context.toString());
      try {
        if (batch.isEmpty()) {
          logger.info("LoggingStreamObserver: keepalive");
        } else {
          final List<String> events = batch.events();
          logger.info(
              "LoggingStreamObserver events processing count {} =====================================",
              events.size());
          for (String event : events) {
            logger.info("LoggingStreamObserver received event: {}", event);
          }
          offsetObserver.onNext(record.streamCursorContext());
          logger.info(
              "LoggingStreamObserver events processed =====================================");
        }
      } finally {
        MDC.remove("cursor_context");
      }
    } catch (NakadiException e) {
      throw e;
    } catch (Exception e) {
      throw new NakadiException(Problem.localProblem(e.getMessage(), ""), e);
    }
  }

  @Override public Optional<Long> requestBackPressure() {
    return Optional.empty();
  }

  @Override public Optional<Integer> requestBuffer() {
    return Optional.empty();
  }
}
