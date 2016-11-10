package nakadi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExecutorServiceSupport {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  static void shutdown(ExecutorService executorService) {
    executorService.shutdown();

    try {
      if (!executorService.awaitTermination(8, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
        if (!executorService.awaitTermination(16, TimeUnit.SECONDS)) {
          logger.info(String.format("executor service did not shutdown cleanly %s",
              executorService.toString()));
        }
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
