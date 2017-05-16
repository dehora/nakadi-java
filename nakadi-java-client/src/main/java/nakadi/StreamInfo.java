package nakadi;

import java.util.Map;
import java.util.Objects;

/**
 * Represents stream information from the server
 */
public class StreamInfo {

  private Map<String, Object> data;

  /**
   * @return throws a {@link UnsupportedOperationException}
   */
  public Map<String, Object> data() {
    //todo: map this as per metrics, server defintion is an empty object
    throw new UnsupportedOperationException(
        "StreamInfo has no definition, needs a custom serializer");
  }

  @Override public int hashCode() {
    return Objects.hash(data);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamInfo that = (StreamInfo) o;
    return Objects.equals(data, that.data);
  }

  @Override public String toString() {
    return "StreamInfo{" + "data=" + data +
        '}';
  }
}
