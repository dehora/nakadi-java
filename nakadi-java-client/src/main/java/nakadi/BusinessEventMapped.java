package nakadi;

import java.util.Map;
import java.util.Objects;

/**
 * A business event that maps custom data into a dedicated field.
 * <p></p>
 * The BusinessEvent in the API is effectively a raw object with one well-known field called
 * "metadata". Because the API's event definition doesn't provide a placeholder field to marshal
 * the custom data it's not possible to define a object for it that isn't a HashMap or something
 * like a raw string.
 * <p></p>
 * {@link BusinessEventMapped} works around this by marshalling custom fields found on the top level
 * JSON event into the {@link BusinessEventMapped#data} Map. As such it doesn't exactly represent
 * the data on the wire.
 */
public class BusinessEventMapped implements Event {

  private Map<String, Object> data;
  private EventMetadata metadata;

  /**
   * The mapped data
   */
  public BusinessEventMapped data(Map<String, Object> data) {
    this.data = data;
    return this;
  }

  public Map<String, Object> data() {
    return data;
  }

  /**
   * The event metadata
   */
  public BusinessEventMapped metadata(EventMetadata metadata) {
    this.metadata = metadata;
    return this;
  }

  public EventMetadata metadata() {
    return metadata;
  }

  @Override public int hashCode() {
    return Objects.hash(metadata, data);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BusinessEventMapped that = (BusinessEventMapped) o;
    return Objects.equals(metadata, that.metadata) &&
        Objects.equals(data, that.data);
  }

  @Override public String toString() {
    return "BusinessEventMapped{" + "metadata=" + metadata +
        ", data=" + data +
        '}';
  }
}
