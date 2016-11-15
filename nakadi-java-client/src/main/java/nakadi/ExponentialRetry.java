package nakadi;

import java.util.concurrent.TimeUnit;

public class ExponentialRetry implements RetryPolicy {

  static final int DEFAULT_INITIAL_INTERVAL_MILLIS = 1000;
  static final int DEFAULT_MAX_INTERVAL_MILLIS = 32000;
  static final int DEFAULT_MAX_ATTEMPTS = Integer.MAX_VALUE;

  long workingInterval;
  long initialInterval;
  long maxInterval;
  int maxAttempts;
  long workingAttempts = 1;
  TimeUnit unit;

  public static Builder newBuilder() {
    return new Builder();
  }

  private ExponentialRetry() {
  }

  ExponentialRetry(Builder builder) {
    this.initialInterval = builder.initialInterval;
    this.maxInterval = builder.maxInterval;
    this.maxAttempts = builder.maxAttempts;
    this.unit = builder.unit;
    this.workingInterval = initialInterval;
  }

  public long initialInterval() {
    return initialInterval;
  }


  public long maxIntervalMillis() {
    return maxInterval;
  }

  public int maxAttempts() {
    return maxAttempts;
  }

  public boolean isFinished() {
    return workingAttempts >= maxAttempts;
  }

  public long nextBackoffMillis() {
    if(isFinished()) {
      return STOP;
    }

    workingInterval = unit.toMillis(workingInterval) * (workingAttempts * workingAttempts);
    workingAttempts++;

    if(workingInterval < 0 ) {
      workingInterval = unit.toMillis(maxInterval);
    }

    if(workingInterval > maxInterval) {
      workingInterval = unit.toMillis(maxInterval);
    }

    return workingInterval;
  }

  @Override public String toString() {
    return "ExponentialRetry{" + "workingInterval=" + workingInterval +
        ", initialInterval=" + initialInterval +
        ", maxInterval=" + maxInterval +
        ", maxAttempts=" + maxAttempts +
        ", workingAttempts=" + workingAttempts +
        ", unit=" + unit +
        '}';
  }

  public static class Builder {

    private long initialInterval = DEFAULT_INITIAL_INTERVAL_MILLIS;
    private long maxInterval = DEFAULT_MAX_INTERVAL_MILLIS;
    private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
    private final TimeUnit unit = TimeUnit.MILLISECONDS;

    private Builder() {
    }

    public Builder initialInterval(long initialInterval, TimeUnit unit) {
      NakadiException.throwNonNull(unit, "Please provide a TimeUnit");
      this.initialInterval = unit.toMillis(initialInterval);
      return this;
    }

    public Builder maxInterval(long maxInterval, TimeUnit unit) {
      NakadiException.throwNonNull(unit, "Please provide a TimeUnit");
      this.maxInterval = unit.toMillis(maxInterval);;
      return this;
    }

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public ExponentialRetry build() {
      return new ExponentialRetry(this);
    }
  }
}
