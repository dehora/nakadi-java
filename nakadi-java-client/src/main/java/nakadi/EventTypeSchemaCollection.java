package nakadi;

import java.util.List;

/**
 * A collection of event type schemas
 */
@Experimental
public class EventTypeSchemaCollection extends ResourceCollection<EventTypeSchema> {

  private final EventTypeResourceReal eventTypeResource;

  public EventTypeSchemaCollection(List<EventTypeSchema> items, List<ResourceLink> links,
      EventTypeResourceReal eventTypeResource, NakadiClient client) {
    super(items, links, client);
    this.eventTypeResource = eventTypeResource;
  }

  public ResourceCollection<EventTypeSchema> fetchPage(String url) {
    return eventTypeResource.loadSchemaPage(url);
  }
}
