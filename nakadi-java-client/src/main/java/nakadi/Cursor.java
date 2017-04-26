package nakadi;

import java.util.Objects;
import java.util.Optional;

/**
 * Represent a Nakadi cursor.
 * <p>
 * This class also implements the SubscriptionCursor model used in the Subscription API. All
 * cursors have a partition and offset, the SubscriptionCursor also has an eventType and
 * cursorToken.  The {@link #isSubscriptionCursor} method can be used to check.
 */
public class Cursor {

  private String partition;
  private String offset;
  private String eventType;
  private String cursorToken;

  public Cursor() {
  }

  /**
   * Constructor representing the standard Cursor model.
   *
   * @param partition the partition for the cursor
   * @param offset the current offset
   */
  public Cursor(String partition, String offset) {
    this.partition = partition;
    this.offset = offset;
  }

  /**
   * Constructor representing the SubscriptionCursor model.
   *
   * @param partition the partition for the cursor
   * @param offset the current offset
   * @param eventType the event type for the cursor
   * @param cursorToken the server-defined cursor token
   */
  public Cursor(String partition, String offset, String eventType, String cursorToken) {
    this(partition, offset);
    this.eventType = eventType;
    this.cursorToken = cursorToken;
  }

  /**
   * Constructor representing the Cursor model used for offset based subscriptions.
   *
   * @param partition the partition for the cursor
   * @param offset the current offset
   * @param eventType the event type for the cursor
   */
  public Cursor(String partition, String offset, String eventType) {
    this(partition, offset, eventType, null);
  }

  /**
   * @return the event type if a named event stream, or {@link Optional#empty}
   */
  public Optional<String> eventType() {
    return Optional.ofNullable(eventType);
  }

  /**
   * Set the event type name.
   *
   * @param eventType the event type name
   * @return this cursor
   */
  public Cursor eventType(String eventType) {
    this.eventType = eventType;
    return this;
  }

  /**
   * @return the token is a subscription stream, or {@link Optional#empty}
   */
  public Optional<String> cursorToken() {
    return Optional.ofNullable(cursorToken);
  }

  /**
   * Set the token.
   *
   * @param cursorToken the token
   * @return this cursor
   */
  public Cursor cursorToken(String cursorToken) {
    this.cursorToken = cursorToken;
    return this;
  }

  /**
   * The partition for this batch.
   *
   * @return the partition
   */
  public String partition() {
    return partition;
  }

  /**
   * The offset of this batch.
   *
   * @return the offset
   */
  public String offset() {
    return offset;
  }

  /**
   * Set the batch partition
   *
   * @param partition the partition
   * @return this cursor
   */
  public Cursor partition(String partition) {
    this.partition = partition;
    return this;
  }

  /**
   * Set the cursor offset
   *
   * @param offset the offset
   * @return this cursor
   */
  public Cursor offset(String offset) {
    this.offset = offset;
    return this;
  }

  /**
   * Detect if this is a cursorfor a susbcription stream.
   *
   * @return true if a subscription, false if a named event stream
   */
  boolean isSubscriptionCursor() {
    return eventType().isPresent() && cursorToken().isPresent();
  }

  @Override public int hashCode() {
    return Objects.hash(partition, offset, eventType, cursorToken);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Cursor cursor = (Cursor) o;
    return Objects.equals(partition, cursor.partition) &&
        Objects.equals(offset, cursor.offset) &&
        Objects.equals(eventType, cursor.eventType) &&
        Objects.equals(cursorToken, cursor.cursorToken);
  }

  @Override public String toString() {
    return "Cursor={" + "partition='" + partition + '\'' +
        ", offset='" + offset + '\'' +
        ", eventType='" + eventType + '\'' +
        ", cursorToken='" + cursorToken + '\'' +
        '}';
  }
}
