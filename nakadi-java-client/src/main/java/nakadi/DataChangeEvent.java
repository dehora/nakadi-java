package nakadi;

import java.util.Objects;

/**
 * A data change event category.
 *
 * @param <T> the generic type of the custom data for the event
 */
public class DataChangeEvent<T> implements Event {

  private T data;
  private EventMetadata metadata;
  private String dataType;
  private Op dataOp;

  public EventMetadata metadata() {
    return metadata;
  }

  public DataChangeEvent<T> metadata(EventMetadata metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * @return the data type
   */
  public String dataType() {
    return dataType;
  }

  /**
   * Set the data type.
   *
   * @param dataType the data type
   * @return this
   */
  public DataChangeEvent<T> dataType(String dataType) {
    this.dataType = dataType;
    return this;
  }

  /**
   * @return the data operation
   */
  public Op op() {
    return dataOp;
  }

  /**
   * Set  the data operation.
   *
   * @param op  the data operation
   * @return this
   */
  public DataChangeEvent<T> op(Op op) {
    this.dataOp = op;
    return this;
  }

  /**
   * @return the event data
   */
  public T data() {
    return data;
  }

  /**
   * Set  the event data.
   *
   * @param data  the event data
   * @return this
   */
  public DataChangeEvent<T> data(T data) {
    this.data = data;
    return this;
  }

  @Override public int hashCode() {
    return Objects.hash(metadata, dataType, dataOp, data);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataChangeEvent that = (DataChangeEvent) o;
    return Objects.equals(metadata, that.metadata) &&
        Objects.equals(dataType, that.dataType) &&
        dataOp == that.dataOp &&
        Objects.equals(data, that.data);
  }

  @Override public String toString() {
    return "DataChangeEvent{" + "metadata=" + metadata +
        ", dataType='" + dataType + '\'' +
        ", dataOp=" + dataOp +
        ", data=" + data +
        '}';
  }

  /**
   * The allowed operation types supported by the API.
   */
  public enum Op {
    C, U, D, S
  }
}
