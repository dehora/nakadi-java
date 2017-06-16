package nakadi;

import java.util.List;

/**
 * Represents the event type stats for an API {@link Subscription}.
 *
 * @see nakadi.Subscription
 */
public class SubscriptionEventTypeStatsCollection
    extends ResourceCollection<SubscriptionEventTypeStats> {

  private final SubscriptionResourceReal subscriptionResource;

  SubscriptionEventTypeStatsCollection(List<SubscriptionEventTypeStats> items,
      List<ResourceLink> links, SubscriptionResourceReal subscriptionResource, NakadiClient client) {
    super(items, links, client);
    this.subscriptionResource = subscriptionResource;
  }

  public ResourceCollection<SubscriptionEventTypeStats> fetchPage(String url) {
    return subscriptionResource.loadStatsPage(url);
  }
}
