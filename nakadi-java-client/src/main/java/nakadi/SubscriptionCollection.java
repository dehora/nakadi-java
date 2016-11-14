package nakadi;

import java.util.List;

/**
 * Represents a collection of subscriptions.
 *
 * @see nakadi.Subscription
 */
public class SubscriptionCollection extends ResourceCollection<Subscription> {

  private final SubscriptionResourceReal subscriptionResource;

  SubscriptionCollection(List<Subscription> items, List<ResourceLink> links,
      SubscriptionResourceReal subscriptionResource) {
    super(items, links);
    this.subscriptionResource = subscriptionResource;
  }

  public ResourceCollection<Subscription> fetchPage(String url) {
    return subscriptionResource.loadPage(url);
  }
}
