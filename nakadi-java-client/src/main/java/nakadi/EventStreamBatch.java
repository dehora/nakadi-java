package nakadi;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;

/*
the 3 event type structures are disjoint
 */
public class EventStreamBatch<T> {

  private static final StreamInfo SENTINEL_STREAM_INFO = new StreamInfo();

  private final Cursor cursor;
  private volatile StreamInfo info;
  private volatile List<T> events;

  public EventStreamBatch(Cursor cursor, StreamInfo info, List<T> events) {
    Objects.requireNonNull(cursor);
    this.cursor = cursor;
    this.info = info;
    this.events = events;
  }

  public Cursor cursor() {
    return cursor;
  }

  public StreamInfo info() {
    // gson doesn't use the constructor, check this here
    if (info == null) {
      this.info = SENTINEL_STREAM_INFO;
    }
    return info;
  }

  public boolean isEmpty() {
    return events().isEmpty();
  }

  public List<T> events() {
    // gson doesn't use the constructor, check this here
    if (events == null) {
      this.events = Lists.newArrayList(); // can't make this a constant because of <T>
    }
    return events;
  }

  @Override public int hashCode() {
    return Objects.hash(cursor, info(), events());
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventStreamBatch<?> that = (EventStreamBatch<?>) o;
    return Objects.equals(cursor, that.cursor) &&
        Objects.equals(info(), that.info()) &&
        Objects.equals(events(), that.events());
  }

  @Override public String toString() {
    return "EventStreamBatch{" + "cursor=" + cursor +
        ", info=" + info() +
        ", events=" + events() +
        '}';
  }
}
