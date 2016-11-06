package nakadi;

import java.util.List;

public class SubscriptionCollection extends ResourceCollection<Subscription> {

  private final SubscriptionResource subscriptionResource;

  public SubscriptionCollection(List<Subscription> items, List<ResourceLink> links,
      SubscriptionResource subscriptionResource) {
    super(items, links);
    this.subscriptionResource = subscriptionResource;
  }

  public ResourceCollection<Subscription> fetchPage(String url) {
    return subscriptionResource.loadPage(url);
  }
}
