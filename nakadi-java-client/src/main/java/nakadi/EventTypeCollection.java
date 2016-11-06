package nakadi;

import java.util.List;

public class EventTypeCollection extends ResourceCollection<EventType> {

  private final EventTypeResource eventTypeResource;

  public EventTypeCollection(List<EventType> items, List<ResourceLink> links,
      EventTypeResource eventTypeResource) {
    super(items, links);
    this.eventTypeResource = eventTypeResource;
  }

  public ResourceCollection<EventType> fetchPage(String url) {
    return eventTypeResource.loadPage(url);
  }
}
