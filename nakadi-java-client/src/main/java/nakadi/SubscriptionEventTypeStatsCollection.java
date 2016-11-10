package nakadi;

import java.util.List;

/**
 * Represents the event type stats for an API {@link Subscription}.
 *
 * @see nakadi.Subscription
 */
public class SubscriptionEventTypeStatsCollection
    extends ResourceCollection<SubscriptionEventTypeStats> {
  private final SubscriptionResource subscriptionResource;

  public SubscriptionEventTypeStatsCollection(List<SubscriptionEventTypeStats> items,
      List<ResourceLink> links, SubscriptionResource subscriptionResource) {
    super(items, links);
    this.subscriptionResource = subscriptionResource;
  }

  public ResourceCollection<SubscriptionEventTypeStats> fetchPage(String url) {
    return subscriptionResource.loadStatsPage(url);
  }
}
