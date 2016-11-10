package nakadi;

import java.util.Objects;

/**
 * Represents an event and its event type.
 *
 * @param <E> the type of the event
 */
public class EventRecord<E extends Event> {

  private final String eventType;
  private final E event;

  public EventRecord(String eventType, E event) {
    this.eventType = eventType;
    this.event = event;
  }

  /**
   * @return the event type
   */
  public String eventType() {
    return eventType;
  }

  /**
   * @return the event
   */
  public E event() {
    return event;
  }

  @Override public int hashCode() {
    return Objects.hash(eventType, event);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventRecord<?> that = (EventRecord<?>) o;
    return Objects.equals(eventType, that.eventType) &&
        Objects.equals(event, that.event);
  }

  @Override public String toString() {
    return "EventRecord{" + "eventTypeName='" + eventType + '\'' +
        ", event=" + event +
        '}';
  }
}
