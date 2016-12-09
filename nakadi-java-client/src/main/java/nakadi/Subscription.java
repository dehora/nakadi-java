package nakadi;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Represents an API subscription.
 *
 * <p>Subscriptions are used to consume event streams in conjunction with
 * support for checkpointing offsets on the server.</p>
 *
 * @see nakadi.StreamProcessor
 */
public class Subscription {

  private String id;
  private String owningApplication;
  private String consumerGroup;
  private String readFrom;
  private List<String> eventTypes = new ArrayList<>();
  private OffsetDateTime createdAt;

  /**
   * @return the subscription id
   */
  public String id() {
    return id;
  }

  Subscription id(String id) {
    this.id = id;
    return this;
  }

  /**
   * @return the owning application for the subscription
   */
  public String owningApplication() {
    return owningApplication;
  }

  /**
   * Set the owning application for the subscription.
   *
   * @param owningApplication the owning application for the subscription
   * @return this
   */
  public Subscription owningApplication(String owningApplication) {
    NakadiException.throwNonNull(owningApplication, "Please supply a non-null cowning application");
    this.owningApplication = owningApplication;
    return this;
  }

  /**
   * @return the name of the subscription's consumer group
   */
  public String consumerGroup() {
    return consumerGroup;
  }

  /**
   * Set the name of the subscription's consumer group.
   *
   * @param consumerGroup the name of the subscription's consumer group
   * @return this
   */
  public Subscription consumerGroup(String consumerGroup) {
    NakadiException.throwNonNull(consumerGroup, "Please supply a non-null consumer group name");
    this.consumerGroup = consumerGroup;
    return this;
  }

  /**
   * Where to read from.
   *
   * @return Where to read from, eg 'begin' 'end'.
   */
  public String readFrom() {
    return readFrom;
  }

  /**
   * Set where to read from.
   *
   * <p>2016-11-10: Known values are:</p>
   *
   * <ul>
   *   <li><code>begin</code>: read from the oldest available event.</li>
   *   <li><code>end</code>: read from the most recent offset. </li>
   * </ul>
   *
   * @param readFrom where to read from.
   * @return this
   */
  public Subscription readFrom(String readFrom) {
    NakadiException.throwNonNull(readFrom, "Please supply a non-null read from value");
    this.readFrom = readFrom;
    return this;
  }

  /**
   * @return The event types the subscription wants.
   */
  public List<String> eventTypes() {
    return eventTypes;
  }

  /**
   * Add the event types the subscription wants.
   * <p>
   *   2016-11-10: the API only supports working with a single event.
   * </p>
   * @param eventTypeNames
   * @return
   */
  public Subscription eventTypes(String... eventTypeNames) {
    NakadiException.throwNonNull(eventTypeNames, "Please supply a non-null list of event type names");
    this.eventTypes.addAll(Arrays.asList(eventTypeNames));
    return this;
  }

  /**
   * Add an event types the subscription wants.
   * <p>
   *   2016-11-10: the API only supports working with a single event.
   * </p>
   * @param eventTypeName
   * @return
   */
  public Subscription eventType(String eventTypeName) {
    NakadiException.throwNonNull(eventTypeName, "Please supply a non-null event type name");
    this.eventTypes.add(eventTypeName);
    return this;
  }

  /**
   * @return the time the subscription was created.
   */
  public OffsetDateTime createdAt() {
    return createdAt;
  }

  @Override public int hashCode() {
    return Objects.hash(id, owningApplication, consumerGroup, readFrom, eventTypes, createdAt);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Subscription that = (Subscription) o;
    return Objects.equals(id, that.id) &&
        Objects.equals(owningApplication, that.owningApplication) &&
        Objects.equals(consumerGroup, that.consumerGroup) &&
        Objects.equals(readFrom, that.readFrom) &&
        Objects.equals(eventTypes, that.eventTypes) &&
        Objects.equals(createdAt, that.createdAt);
  }

  @Override public String toString() {
    return "Subscription{" + "id='" + id + '\'' +
        ", owningApplication='" + owningApplication + '\'' +
        ", consumerGroup='" + consumerGroup + '\'' +
        ", readFrom='" + readFrom + '\'' +
        ", eventTypes=" + eventTypes +
        ", createdAt=" + createdAt +
        '}';
  }
}
