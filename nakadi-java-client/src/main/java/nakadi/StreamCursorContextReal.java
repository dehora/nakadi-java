package nakadi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

class StreamCursorContextReal implements StreamCursorContext {

  private static final Map<String, String> SENTINEL = new HashMap<>();
  private static final Map<String, String> U_SENTINEL = Collections.unmodifiableMap(SENTINEL);

  private final Cursor cursor;
  private Map<String, String> context;

  StreamCursorContextReal(Cursor cursor) {
    this(cursor, U_SENTINEL);
  }

  StreamCursorContextReal(Cursor cursor, Map<String, String> context) {
    this.cursor = cursor;
    this.context = context;
  }

  @Override public Cursor cursor() {
    return cursor;
  }

  @Override public Map<String, String> context() {
    if (context == U_SENTINEL) {
      return context;
    }
    return Collections.unmodifiableMap(context);
  }

  @Override public int hashCode() {
    return Objects.hash(cursor, context);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamCursorContextReal that = (StreamCursorContextReal) o;
    return Objects.equals(cursor, that.cursor) &&
        Objects.equals(context, that.context);
  }

  @Override public String toString() {
    return "StreamCursorContext={" + "cursor=" + cursor +
        ", context=" + context +
        '}';
  }
}
