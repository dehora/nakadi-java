package nakadi;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Represents an event type's options as supported by the API.
 */
public class EventTypeOptions {

  private long retentionTime;

  /**
   * @return how long to retain events
   */
  public long retentionTimeMillis() {
    return retentionTime;
  }

  /**
   * Set how long to retain events. Will be converted to millis by the supplied TimeUnit.
   *
   * @param retentionTime the time
   * @param unit the unit of time
   * @return this
   */
  public EventTypeOptions retentionTime(long retentionTime, TimeUnit unit) {
    NakadiException.throwNonNull(retentionTime, "Please provide a retention time unit");
    this.retentionTime = unit.toMillis(retentionTime);
    return this;
  }

  @Override public int hashCode() {
    return Objects.hash(retentionTime);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventTypeOptions that = (EventTypeOptions) o;
    return retentionTime == that.retentionTime;
  }

  @Override public String toString() {
    return "EventTypeOptions{" + "retentionTime=" + retentionTime +
        '}';
  }
}
