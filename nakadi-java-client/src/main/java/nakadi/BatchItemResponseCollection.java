package nakadi;

import java.util.List;

/**
 * The result of a partial failure to post events.
 */
public class BatchItemResponseCollection extends ResourceCollection<BatchItemResponse> {

  BatchItemResponseCollection(List<BatchItemResponse> items, List<ResourceLink> links) {
    super(items, links);
  }
  
  public ResourceCollection<BatchItemResponse> fetchPage(String url) {
    throw new UnsupportedOperationException("Paging batch item responses is not supported");
  }
}
