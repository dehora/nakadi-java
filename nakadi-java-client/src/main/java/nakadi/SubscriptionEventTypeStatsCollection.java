package nakadi;

import java.util.List;

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
