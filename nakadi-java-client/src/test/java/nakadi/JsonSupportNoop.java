package nakadi;

import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Objects;

public class JsonSupportNoop implements JsonSupport {

  String name;

  public JsonSupportNoop(String name) {
    this.name = name;
  }

  @Override public String toJsonCompressed(Object o) {
    return null;
  }

  @Override public String toJson(Object o) {
    return null;
  }

  @Override public byte[] toJsonBytes(Object o) {
    return new byte[0];
  }

  @Override public <T> T fromJson(String raw, Class<T> c) {
    return null;
  }

  @Override public <T> T fromJson(String raw, Type tType) {
    return null;
  }

  @Override public <T> T fromJson(Reader r, Class<T> c) {
    return null;
  }

  @Override public <T> T fromJson(Reader r, Type tType) {
    return null;
  }

  @Override public <T> Object transformEventRecord(EventRecord<T> er) {
    return null;
  }

  @Override public <T> EventStreamBatch<T> marshalEventStreamBatch(String raw, Type type) {
    return null;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JsonSupportNoop that = (JsonSupportNoop) o;
    return Objects.equals(name, that.name);
  }

  @Override public int hashCode() {
    return Objects.hash(name);
  }
}
