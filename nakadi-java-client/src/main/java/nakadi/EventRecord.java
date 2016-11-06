package nakadi;

import java.util.Objects;

public class EventRecord<E extends Event> {

  private final String topic;
  private final E event;

  public EventRecord(String topic, E event) {
    this.topic = topic;
    this.event = event;
  }

  public String topic() {
    return topic;
  }

  public E event() {
    return event;
  }

  @Override public int hashCode() {
    return Objects.hash(topic, event);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventRecord<?> that = (EventRecord<?>) o;
    return Objects.equals(topic, that.topic) &&
        Objects.equals(event, that.event);
  }

  @Override public String toString() {
    return "EventRecord{" + "eventTypeName='" + topic + '\'' +
        ", event=" + event +
        '}';
  }
}
