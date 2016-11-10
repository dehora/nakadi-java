package nakadi;

import java.util.List;

/**
 * The result of committing a cursor to the subscription API.
 */
public class CursorCommitResultCollection extends ResourceCollection<CursorCommitResult> {

  private final SubscriptionResource resource;

  /**
   * @param items the results
   * @param links links for pagination
   * @param resource a subscription resource
   */
  public CursorCommitResultCollection(List<CursorCommitResult> items, List<ResourceLink> links,
      SubscriptionResource resource) {
    super(items, links);
    this.resource = resource;
  }

  public ResourceCollection<CursorCommitResult> fetchPage(String url) {
    return resource.loadCursorCommitPage(url);
  }
}

