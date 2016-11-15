package nakadi;

import java.util.Objects;

/**
 * <p>An undefined event that maps custom data into a dedicated field.</p>
 *
 * <p> The UndefinedEvent in the API is effectively a raw object. Because the API's event definition
 * doesn't provide a placeholder field to marshal the custom data it's not possible to define a
 * object for it that isn't a HashMap or something like a raw string. </p>
 *
 * <p>{@link UndefinedEventMapped} works around this by marshalling custom fields found on the top
 * level JSON event into the {@link UndefinedEventMapped#data} field. As such it doesn't exactly
 * represent the data on the wire; instead the on wire representation is in the data field. </p>
 */
public class UndefinedEventMapped<T> implements Event {

  private T data;

  public UndefinedEventMapped() {
  }

  public UndefinedEventMapped(T data) {
    this.data = data;
  }

  /**
   * The mapped data
   */
  public UndefinedEventMapped<T> data(T data) {
    this.data = data;
    return this;
  }

  public T data() {
    return data;
  }

  @Override public int hashCode() {
    return Objects.hash(data);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UndefinedEventMapped that = (UndefinedEventMapped) o;
    return Objects.equals(data, that.data);
  }

  @Override public String toString() {
    return "UndefinedEventMapped{" + "data=" + data +
        '}';
  }
}
