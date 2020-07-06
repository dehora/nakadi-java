package nakadi;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExponentialRetryTest {

  @Test
  public void tempusFugit_1() throws Exception {
    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .initialInterval(11, TimeUnit.MILLISECONDS)
        .maxAttempts(Integer.MAX_VALUE)
        .maxInterval(20, TimeUnit.MILLISECONDS)
        .percentOfMaxIntervalForJitter(20)
        .maxTime(3, TimeUnit.SECONDS)
        .build();
    runRetries(exponentialRetry);
    validateTimeoutState(exponentialRetry);
  }

  @Test
  public void tempusFugit_2() throws Exception {
    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .maxTime(3, TimeUnit.SECONDS)
        .maxInterval(100, TimeUnit.MILLISECONDS)
        .build();
    runRetries(exponentialRetry);
    validateTimeoutState(exponentialRetry);
  }

  private void validateTimeoutState(ExponentialRetry exponentialRetry) {
    assertTrue(exponentialRetry.workingTime() >= exponentialRetry.maxTime);
    assertTrue(exponentialRetry.workingAttempts < exponentialRetry.maxAttempts);
  }

  @Test
  public void annumero_1() throws Exception {
    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .initialInterval(101, TimeUnit.MILLISECONDS)
        .maxAttempts(20)
        .maxInterval(100, TimeUnit.MILLISECONDS)
        .maxTime(Integer.MAX_VALUE, TimeUnit.SECONDS)
        .build();
    runRetries(exponentialRetry);
    validateRetriesExceededState(exponentialRetry);
  }

  @Test
  public void annumero_2() throws Exception {
    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
            .maxAttempts(3)
            .maxInterval(100, TimeUnit.MILLISECONDS)
            .build();
    runRetries(exponentialRetry);
    validateRetriesExceededState(exponentialRetry);
  }

  @Test
  public void workingTimeCalculation() {
    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
            .maxInterval(100, TimeUnit.MILLISECONDS)
            .maxTime(110, TimeUnit.MILLISECONDS)
            .build();
    exponentialRetry.nextBackOffMillis(1);
    assertFalse(exponentialRetry.isFinished());
    exponentialRetry.nextBackOffMillis(100);
    assertFalse(exponentialRetry.isFinished());
    exponentialRetry.nextBackOffMillis(101);
    assertFalse(exponentialRetry.isFinished());
    exponentialRetry.nextBackOffMillis(111);
    assertTrue(exponentialRetry.isFinished());
  }

  private void validateRetriesExceededState(ExponentialRetry exponentialRetry) {
    assertTrue(exponentialRetry.workingTime() < exponentialRetry.maxTime);
    assertTrue(exponentialRetry.workingAttempts >= exponentialRetry.maxAttempts);
  }

  private void runRetries(ExponentialRetry exponentialRetry) throws InterruptedException {
    while(true) {
      long l = exponentialRetry.nextBackoffMillis();
      if(l == RetryPolicy.STOP) {
        break;
      }
      // This does not hold: l >= exponentialRetry.initialInterval()
      assertTrue(l <= exponentialRetry.maxIntervalMillis());

      Thread.sleep(l);
    }
    assertTrue(exponentialRetry.isFinished());
  }

}
