package nakadi;

import com.google.common.collect.Lists;
import java.util.List;

public class CursorDistanceCollection extends ResourceCollection<CursorDistance> {

  private static final CursorDistanceCollection sentinel = new
      CursorDistanceCollection(Lists.newArrayList(), Lists.newArrayList());

  /**
   * @param items the results
   * @param links links for pagination
   */
  CursorDistanceCollection(
      List<CursorDistance> items, List<ResourceLink> links) {
    super(items, links);
  }

  @Override public ResourceCollection<CursorDistance> fetchPage(String url) {
    return sentinel;
  }

  @Override public boolean hasNextLink() {
    // there's no pagination on this collection type
    return false;
  }

  @Override public ResourceCollection<CursorDistance> nextPage() {
    return sentinel;
  }
}
