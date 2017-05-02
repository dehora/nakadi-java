package nakadi;

import java.util.Objects;

/**
 * Represents the distance between two cursors as calculated by the server.
 *
 */
public class CursorDistance {

  private Cursor initialCursor;
  private Cursor finalCursor;
  private Long distance;

  CursorDistance(Cursor initialCursor, Cursor finalCursor, Long distance) {
    this.initialCursor = initialCursor;
    this.finalCursor = finalCursor;
    this.distance = distance;
  }

  public CursorDistance() {
  }

  /**
   * The initial cursor.
   *
   * @return the initial cursor.
   */
  public Cursor initialCursor() {
    return initialCursor;
  }

  /**
   * Set the initial cursor.
   *
   * @param initialCursor The initial cursor.
   * @return this
   */
  public CursorDistance initialCursor(Cursor initialCursor) {
    this.initialCursor = initialCursor;
    return this;
  }

  /**
   * The final cursor.
   *
   * @return the final cursor.
   */
  public Cursor finalCursor() {
    return finalCursor;
  }

  /**
   * Set the final cursor.
   *
   * @param finalCursor the final cursor.
   * @return this
   */
  public CursorDistance finalCursor(Cursor finalCursor) {
    this.finalCursor = finalCursor;
    return this;
  }

  /**
   * The calculated number of events between two cursors.
   *
   * @return the number of events
   */
  public Long distance() {
    return distance;
  }

  CursorDistance distance(Long distance) {
    this.distance = distance;
    return this;
  }

  @Override public String toString() {
    return "CursorDistance{" + "initialCursor=" + initialCursor +
        ", finalCursor=" + finalCursor +
        ", distance=" + distance +
        '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CursorDistance that = (CursorDistance) o;
    return Objects.equals(initialCursor, that.initialCursor) &&
        Objects.equals(finalCursor, that.finalCursor) &&
        Objects.equals(distance, that.distance);
  }

  @Override public int hashCode() {
    return Objects.hash(initialCursor, finalCursor, distance);
  }
}
