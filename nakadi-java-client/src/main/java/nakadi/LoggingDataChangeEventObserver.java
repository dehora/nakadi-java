package nakadi;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to log data events from a stream with Map data.
 */
public class LoggingDataChangeEventObserver
    extends StreamObserverBackPressure<DataChangeEvent<Map<String, Object>>> {

  private static final Logger logger = LoggerFactory.getLogger(LoggingDataChangeEventObserver.class);

  @Override public void onStart() {
    logger.info("onStart");
  }

  @Override public void onStop() {
    logger.info("onStop");
  }

  @Override public void onCompleted() {
    logger.info("onCompleted {}", Thread.currentThread().getName());
  }

  @Override public void onError(Throwable e) {
    logger.info("onError {} {}", e.getMessage(), Thread.currentThread().getName());
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void onNext(StreamBatchRecord<DataChangeEvent<Map<String, Object>>> record) {
    final StreamOffsetObserver offsetObserver = record.streamOffsetObserver();
    final StreamBatch<DataChangeEvent<Map<String, Object>>> batch = record.streamBatch();
    final StreamCursorContext cursor = record.streamCursorContext();

    logger.info("partition: {} ------------- {}",
        cursor.cursor().partition(), Thread.currentThread().getName());

    if (batch.isEmpty()) {
      logger.info("partition: %s empty batch", cursor.cursor().partition());
    } else {
      final List<DataChangeEvent<Map<String, Object>>> events = batch.events();
      for (DataChangeEvent<Map<String, Object>> event : events) {
        int hashCode = event.hashCode();
        logger.info("{} event ------------- ", hashCode);
        logger.info("{} metadata: {} ", hashCode, event.metadata());
        logger.info("{} op: {} ", hashCode, event.op());
        logger.info("{} dataType: {} ", hashCode, event.dataType());
        logger.info("{} data: {} ", hashCode, event.data());
      }
    }
    offsetObserver.onNext(record.streamCursorContext());
  }
}
