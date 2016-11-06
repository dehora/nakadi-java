package nakadi;

import java.util.List;
import java.util.Objects;

class StreamBatchReal<T> implements StreamBatch<T> {

  private final EventStreamBatch<T> eventStreamBatch;

  StreamBatchReal(EventStreamBatch<T> eventStreamBatch) {
    this.eventStreamBatch = eventStreamBatch;
  }

  @Override public Cursor cursor() {
    return eventStreamBatch.cursor();
  }

  @Override public StreamInfo info() {
    return eventStreamBatch.info();
  }

  @Override public List<T> events() {
    return eventStreamBatch.events();
  }

  @Override public boolean isEmpty() {
    return eventStreamBatch.isEmpty();
  }

  @Override public String toString() {
    return "StreamBatchReal{" + "eventStreamBatch=" + eventStreamBatch +
        '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamBatchReal<?> that = (StreamBatchReal<?>) o;
    return Objects.equals(eventStreamBatch, that.eventStreamBatch);
  }

  @Override public int hashCode() {
    return Objects.hash(eventStreamBatch);
  }
}
