package nakadi;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ExponentialRetry implements RetryPolicy {

  public static final int MAX_INTERVAL_MIN_AS_MILLIS = 20;
  public static final int INITIAL_INTERVAL_MIN_AS_MILLIS = 10;
  public static final float PERCENT_OF_MAX_INTERVAL_AS_JITTER = 10.0f;
  static final int DEFAULT_INITIAL_INTERVAL_MILLIS = 1000;
  static final int DEFAULT_MAX_INTERVAL_MILLIS = 32000;
  static final int DEFAULT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  static final long DEFAULT_MAX_TIME = Long.MAX_VALUE;
  private final long initialInterval;
  private final long maxInterval;
  int maxAttempts;
  long workingAttempts = 1;
  long maxTime;
  long workingTime = 0L;
  private long workingInterval;
  private volatile long startTime = 0L;
  private float percentOfMaxIntervalForJitter;

  ExponentialRetry(Builder builder) {
    this.initialInterval = Math.min(builder.maxInterval, builder.initialInterval);
    this.maxInterval = builder.maxInterval;
    this.workingInterval = initialInterval;
    this.maxAttempts = builder.maxAttempts;
    this.maxTime = builder.maxTime;
    this.percentOfMaxIntervalForJitter = builder.percentOfMaxIntervalForJitter;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public long initialInterval() {
    return initialInterval;
  }

  public long maxIntervalMillis() {
    return maxInterval;
  }

  public boolean isFinished() {
    return workingAttempts >= maxAttempts || workingTime >= maxTime;
  }

  public long nextBackoffMillis() {

    if (startTime == 0L) {
      startTime = System.currentTimeMillis();
    } else {
      workingTime += (System.currentTimeMillis() - startTime);
    }

    if (isFinished()) {
      return STOP;
    }

    workingInterval = workingInterval * (workingAttempts * workingAttempts);
    workingAttempts++;

    if (workingInterval <= 0) {
      workingInterval = maxInterval;
    }

    if (initialInterval != workingInterval) {
      workingInterval = Math.min(maxInterval,
          ThreadLocalRandom.current().nextLong(initialInterval, workingInterval));
      if (workingInterval == maxInterval) {
        /*
         avoid fixating on the max by picking a wait between it and a percentage less than it
         We have some retries that can run for very long periods, eg consumers and will eventually
         settle and coordinate on the max value
          */
        final long percentMax = (long) (maxInterval * (percentOfMaxIntervalForJitter / 100.0f));
        workingInterval =
            ThreadLocalRandom.current().nextLong(maxInterval - percentMax, maxInterval);
      }
    }

    return workingInterval;
  }

  public int maxAttempts() {
    return maxAttempts;
  }

  @Override public long workingAttempts() {
    return workingAttempts;
  }

  @Override public String toString() {
    return "ExponentialRetry{" + "workingInterval=" + workingInterval +
        ", initialInterval=" + initialInterval +
        ", maxInterval=" + maxInterval +
        ", maxAttempts=" + maxAttempts +
        ", workingAttempts=" + workingAttempts +
        '}';
  }

  public static class Builder {
    public float percentOfMaxIntervalForJitter = PERCENT_OF_MAX_INTERVAL_AS_JITTER;
    private long initialInterval = DEFAULT_INITIAL_INTERVAL_MILLIS;
    private long maxInterval = DEFAULT_MAX_INTERVAL_MILLIS;
    private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
    private long maxTime = DEFAULT_MAX_TIME;

    private Builder() {
    }

    public Builder initialInterval(long initialInterval, TimeUnit unit) {
      NakadiException.throwNonNull(unit, "Please provide a TimeUnit");
      this.initialInterval = unit.toMillis(initialInterval);
      if (this.initialInterval < INITIAL_INTERVAL_MIN_AS_MILLIS) {
        NakadiException.throwNonNull(null, "Please provide an initial value of at least "
            + INITIAL_INTERVAL_MIN_AS_MILLIS
            + " millis");
      }
      return this;
    }

    public Builder maxInterval(long maxInterval, TimeUnit unit) {
      NakadiException.throwNonNull(unit, "Please provide a TimeUnit");
      this.maxInterval = unit.toMillis(maxInterval);
      if (this.maxInterval < MAX_INTERVAL_MIN_AS_MILLIS) {
        NakadiException.throwNonNull(null, "Please provide a max interval value of at least "
            + MAX_INTERVAL_MIN_AS_MILLIS
            + " millis");
      }
      return this;
    }

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder maxTime(long maxTime, TimeUnit unit) {
      NakadiException.throwNonNull(unit, "Please provide a TimeUnit");
      this.maxTime = unit.toMillis(maxTime);
      return this;
    }

    public Builder percentOfMaxIntervalForJitter(int percentOfMaxIntervalForJitter) {
      this.percentOfMaxIntervalForJitter = percentOfMaxIntervalForJitter + 0.0f;
      return this;
    }

    public ExponentialRetry build() {
      return new ExponentialRetry(this);
    }
  }
}
