package nakadi;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class EventTypeOptions {

  private long retentionTime;

  public long retentionTimeMillis() {
    return retentionTime;
  }

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
