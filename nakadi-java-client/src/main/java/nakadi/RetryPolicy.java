package nakadi;

public interface RetryPolicy {

  long STOP = -1L;

  boolean isFinished();

  long nextBackoffMillis();

  int maxAttempts();
}
