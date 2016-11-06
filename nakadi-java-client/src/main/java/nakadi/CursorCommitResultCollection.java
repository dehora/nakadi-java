package nakadi;

import java.util.List;

public class CursorCommitResultCollection extends ResourceCollection<CursorCommitResult> {

  private final SubscriptionResource resource;

  public CursorCommitResultCollection(List<CursorCommitResult> items, List<ResourceLink> links,
      SubscriptionResource resource) {
    super(items, links);
    this.resource = resource;
  }

  public ResourceCollection<CursorCommitResult> fetchPage(String url) {
    return resource.loadCursorCommitPage(url);
  }
}

