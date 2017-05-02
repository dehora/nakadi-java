package nakadi;

import java.util.List;

public class CursorCollection extends ResourceCollection<Cursor>  {

  private final EventTypeResourceReal resource;

  /**
   * @param items the results
   * @param links links for pagination
   * @param resource a subscription resource
   */
  CursorCollection(
      List<Cursor> items, List<ResourceLink> links, EventTypeResourceReal resource) {
    super(items, links);
    this.resource = resource;
  }

  public ResourceCollection fetchPage(String url) {
    return null;
  }

}
