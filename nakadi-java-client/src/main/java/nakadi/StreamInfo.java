package nakadi;

import java.util.Map;
import java.util.Objects;

public class StreamInfo {

  /*
  todo: StreamInfo has no definition, needs a custom serializer
   */
  private Map data;

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
