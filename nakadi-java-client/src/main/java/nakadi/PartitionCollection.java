package nakadi;

import java.util.List;

public class PartitionCollection extends ResourceCollection<Partition> {

  private final EventTypeResource eventTypeResource;

  public PartitionCollection(List<Partition> items, List<ResourceLink> links,
      EventTypeResource eventTypeResource) {
    super(items, links);
    this.eventTypeResource = eventTypeResource;
  }

  public ResourceCollection<Partition> fetchPage(String url) {
    return eventTypeResource.loadPartitionPage(url);
  }
}
