package nakadi;

import java.util.Objects;

public class PaginationLinks {

  private PaginationLink prev;
  private PaginationLink next;

  public PaginationLink prev() {
    return prev;
  }

  public PaginationLink next() {
    return next;
  }

  @Override public int hashCode() {
    return Objects.hash(prev, next);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PaginationLinks that = (PaginationLinks) o;
    return Objects.equals(prev, that.prev) &&
        Objects.equals(next, that.next);
  }

  @Override public String toString() {
    return "PaginationLinks{" + "prev=" + prev +
        ", next=" + next +
        '}';
  }
}
