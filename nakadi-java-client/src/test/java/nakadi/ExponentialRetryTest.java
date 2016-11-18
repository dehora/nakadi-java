package nakadi;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExponentialRetryTest {

  @Test
  public void tempusFugit() throws Exception {

    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.MILLISECONDS)
        .maxAttempts(Integer.MAX_VALUE)
        .maxInterval(3, TimeUnit.MILLISECONDS)
        .maxTime(3, TimeUnit.SECONDS)
        .build();

    while(true) {
      long l = exponentialRetry.nextBackoffMillis();
      if(l == -1) {
        break;
      }
      Thread.sleep(l);
    }

    assertTrue(exponentialRetry.workingTime >= exponentialRetry.maxTime);
    assertTrue(exponentialRetry.workingAttempts < exponentialRetry.maxAttempts);

    exponentialRetry = ExponentialRetry.newBuilder()
        .maxTime(3, TimeUnit.SECONDS)
        .maxInterval(100, TimeUnit.MILLISECONDS)
        .build();

    while(true) {
      long l = exponentialRetry.nextBackoffMillis();
      if(l == -1) {
        break;
      }
      Thread.sleep(l);
    }

    assertTrue(exponentialRetry.workingTime >= exponentialRetry.maxTime);
    assertTrue(exponentialRetry.workingAttempts < exponentialRetry.maxAttempts);
  }

  @Test
  public void annumero() throws Exception {

    ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .initialInterval(1, TimeUnit.MILLISECONDS)
        .maxAttempts(20)
        .maxInterval(100, TimeUnit.MILLISECONDS)
        .maxTime(Integer.MAX_VALUE, TimeUnit.SECONDS)
        .build();

    while(true) {
      long l = exponentialRetry.nextBackoffMillis();
      if(l == -1) {
        break;
      }
      Thread.sleep(l);
    }

    assertTrue(exponentialRetry.workingTime < exponentialRetry.maxTime);
    assertTrue(exponentialRetry.workingAttempts >= exponentialRetry.maxAttempts);


    exponentialRetry = ExponentialRetry.newBuilder()
        .maxAttempts(3)
        .maxInterval(100, TimeUnit.MILLISECONDS)
        .build();

    while(true) {
      long l = exponentialRetry.nextBackoffMillis();
      if(l == -1) {
        break;
      }
      Thread.sleep(l);
    }

    assertTrue(exponentialRetry.workingTime < exponentialRetry.maxTime);
    assertTrue(exponentialRetry.workingAttempts >= exponentialRetry.maxAttempts);
  }

}