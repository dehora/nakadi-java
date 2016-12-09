package nakadi;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a batch of events emitted from a Stream. Assumes all batches are of a common single
 * type.
 * <p>
 *   Currently the three category types supported by the API are disjoint. The client marks them
 *   with the {@link Event} interface but the are all handled differently when it comes to internal
 *   serdes operations. Even so, this class can carry all three kinds and also to allow users to
 *   define their own data models for events in the stream if they wish.
 * </p>
 * @see nakadi.DataChangeEvent
 * @see nakadi.BusinessEventMapped
 * @see nakadi.UndefinedEventMapped
 * @param <T> the type of the event data contained in the batch.
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

  /**
   * @return The cursor for this batch.
   */
  public Cursor cursor() {
    return cursor;
  }

  /**
   * @return the stream information for this batch.
   */
  public StreamInfo info() {
    // gson doesn't use the constructor, check this here
    if (info == null) {
      this.info = SENTINEL_STREAM_INFO;
    }
    return info;
  }

  /**
   *
   * @return true if the batch is empty (signals a keep-alive batch).
   */
  public boolean isEmpty() {
    return events().isEmpty();
  }

  /**
   * @return the list of events in this batch.
   */
  public List<T> events() {
    // gson doesn't use the constructor, check this here
    if (events == null) {
      this.events = new ArrayList<>(); // can't make this a constant because of <T>
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
