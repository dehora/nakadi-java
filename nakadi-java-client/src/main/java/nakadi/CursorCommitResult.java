package nakadi;

import java.util.Objects;

public class CursorCommitResult {

  private Cursor cursor;
  private String result;

  public Cursor cursor() {
    return cursor;
  }

  public String result() {
    return result;
  }

  @Override public int hashCode() {
    return Objects.hash(cursor, result);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CursorCommitResult that = (CursorCommitResult) o;
    return Objects.equals(cursor, that.cursor) &&
        Objects.equals(result, that.result);
  }

  @Override public String toString() {
    return "CursorCommitResult{" + "cursors=" + cursor +
        ", result='" + result + '\'' +
        '}';
  }
}
