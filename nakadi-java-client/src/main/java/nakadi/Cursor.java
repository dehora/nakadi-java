package nakadi;

import java.util.Objects;
import java.util.Optional;

/**
 * Represent a Nakadi cursor.
 * <p/>
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

  public Optional<String> eventType() {
    return Optional.ofNullable(eventType);
  }

  public Cursor eventType(String eventType) {
    this.eventType = eventType;
    return this;
  }

  public Optional<String> cursorToken() {
    return Optional.ofNullable(cursorToken);
  }

  public Cursor cursorToken(String cursorToken) {
    this.cursorToken = cursorToken;
    return this;
  }

  public String partition() {
    return partition;
  }

  public String offset() {
    return offset;
  }

  public Cursor partition(String partition) {
    this.partition = partition;
    return this;
  }

  public Cursor offset(String offset) {
    this.offset = offset;
    return this;
  }

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
