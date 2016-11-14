package nakadi;

import java.util.List;

/**
 * Represents the cursors for an API {@link Subscription}.
 *
 * @see nakadi.Subscription
 */
public class SubscriptionCursorCollection extends ResourceCollection<Cursor> {

  private final SubscriptionResourceReal subscriptionResource;

  SubscriptionCursorCollection(List<Cursor> items,
      List<ResourceLink> links,
      SubscriptionResourceReal subscriptionResource) {
    super(items, links);
    this.subscriptionResource = subscriptionResource;
  }

  public ResourceCollection<Cursor> fetchPage(String url) {
    return subscriptionResource.loadCursorPage(url);
  }
}
