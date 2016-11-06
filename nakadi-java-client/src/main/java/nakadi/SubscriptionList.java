package nakadi;

import java.util.List;
import java.util.Objects;

public class SubscriptionList {

  private PaginationLinks _links;
  private List<Subscription> items;

  public PaginationLinks links() {
    return _links;
  }

  public List<Subscription> items() {
    return items;
  }

  @Override public int hashCode() {
    return Objects.hash(_links, items);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubscriptionList that = (SubscriptionList) o;
    return Objects.equals(_links, that._links) &&
        Objects.equals(items, that.items);
  }

  @Override public String toString() {
    return "SubscriptionList{" + "_links=" + _links +
        ", items=" + items +
        '}';
  }
}
