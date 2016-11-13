package nakadi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorServiceSupport {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  public static void shutdown(ExecutorService executorService) {
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
