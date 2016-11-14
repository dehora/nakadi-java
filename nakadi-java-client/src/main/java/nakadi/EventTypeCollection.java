package nakadi;

import java.util.List;

/**
 * A collection of event types
 */
public class EventTypeCollection extends ResourceCollection<EventType> {

  private final EventTypeResourceReal eventTypeResource;

  public EventTypeCollection(List<EventType> items, List<ResourceLink> links,
      EventTypeResourceReal eventTypeResource) {
    super(items, links);
    this.eventTypeResource = eventTypeResource;
  }

  public ResourceCollection<EventType> fetchPage(String url) {
    return eventTypeResource.loadPage(url);
  }
}
