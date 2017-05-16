package nakadi;

import java.util.List;

public class CursorCollection extends ResourceCollection<Cursor>  {

  /**
   * @param items the results
   * @param links links for pagination
   */
  CursorCollection(
      List<Cursor> items, List<ResourceLink> links) {
    super(items, links);
  }

  @SuppressWarnings("unchecked") public ResourceCollection fetchPage(String url) {
    return null;
  }

}
