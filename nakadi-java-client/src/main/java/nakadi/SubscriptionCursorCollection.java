package nakadi;

import java.util.List;

public class SubscriptionCursorCollection extends ResourceCollection<Cursor> {

  private final SubscriptionResource subscriptionResource;

  public SubscriptionCursorCollection(List<Cursor> items,
      List<ResourceLink> links,
      SubscriptionResource subscriptionResource) {
    super(items, links);
    this.subscriptionResource = subscriptionResource;
  }

  public ResourceCollection<Cursor> fetchPage(String url) {
    return subscriptionResource.loadCursorPage(url);
  }
}
