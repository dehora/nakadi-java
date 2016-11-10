package nakadi;

public interface PolicyBackoff {

  long STOP = -1L;

  boolean isFinished();

  long nextBackoffMillis();

  int maxAttempts();
}
