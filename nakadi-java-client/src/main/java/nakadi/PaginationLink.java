package nakadi;

import java.net.URI;
import java.util.Objects;

public class PaginationLink {

  private URI href;

  public URI href() {
    return href;
  }

  @Override public int hashCode() {
    return Objects.hash(href);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PaginationLink that = (PaginationLink) o;
    return Objects.equals(href, that.href);
  }

  @Override public String toString() {
    return "PaginationLink{" + "href=" + href +
        '}';
  }
}
