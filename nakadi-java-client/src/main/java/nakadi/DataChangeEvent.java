package nakadi;

import java.util.Objects;

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

  public String dataType() {
    return dataType;
  }

  public DataChangeEvent<T> dataType(String dataType) {
    this.dataType = dataType;
    return this;
  }

  public Op op() {
    return dataOp;
  }

  public DataChangeEvent<T> op(Op op) {
    this.dataOp = op;
    return this;
  }

  public T data() {
    return data;
  }

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

  public enum Op {
    C, U, D, S
  }
}
