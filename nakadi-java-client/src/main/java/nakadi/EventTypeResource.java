package nakadi;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Supports API operations related to event types.
 */
public class EventTypeResource {

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_PARTITIONS = "partitions";
  private static final String APPLICATION_JSON = "application/json";
  private static final Type TYPE = new TypeToken<List<EventType>>() {
  }.getType();
  private static final Type TYPE_P = new TypeToken<List<Partition>>() {
  }.getType();

  private final NakadiClient client;

  EventTypeResource(NakadiClient client) {
    this.client = client;
  }

  /**
   * Create an event type.
   *
   * @param eventType an event type
   * @return a http response
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  public Response create(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    return client.resourceProvider().newResource()
        .requestThrowing(Resource.POST, collectionUri().buildString(), prepareOptions(), eventType);
  }

  /**
   * Update an existing event type
   *
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  public Response update(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventType.name()).buildString();
    return client.resourceProvider().newResource()
        .requestThrowing(Resource.PUT, url, prepareOptions(), eventType);
  }

  /**
   * @param eventTypeName the event type name
   */
  public EventType findByName(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventTypeName).buildString();
    return client.resourceProvider().newResource()
        .requestThrowing(Resource.GET, url, prepareOptions(), EventType.class);
  }

  /**
   * Successful deletes return 200 and no body.
   *
   * @return a http response that will have no body
   */
  public Response delete(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventTypeName).buildString();
    return client.resourceProvider().newResource()
        .requestThrowing(Resource.DELETE, url, prepareOptions());
  }

  /**
   * @return a collection of event types
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  public EventTypeCollection list()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    return loadPage(collectionUri().buildString());
  }

  public PartitionCollection partitions(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    return loadPartitionPage(
        collectionUri().path(eventTypeName).path(PATH_PARTITIONS).buildString());
  }

  public Partition partition(String eventTypeName, String partitionId)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    final String url =
        collectionUri().path(eventTypeName).path(PATH_PARTITIONS).path(partitionId).buildString();
    return client.resourceProvider().newResource()
        .requestThrowing(Resource.GET, url, prepareOptions(), Partition.class);
  }

  EventTypeCollection loadPage(String url) {
    Response response = client.resourceProvider().newResource()
        .requestThrowing(Resource.GET, url,
            ResourceSupport.options(APPLICATION_JSON)
                .tokenProvider(client.resourceTokenProvider()));

    List<EventType> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE);

    return new EventTypeCollection(collection, Lists.newArrayList(), this);
  }

  PartitionCollection loadPartitionPage(String url) {
    Response response = client.resourceProvider().newResource()
        .requestThrowing(Resource.GET, url,
            ResourceSupport.options(APPLICATION_JSON)
                .tokenProvider(client.resourceTokenProvider()));

    List<Partition> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE_P);

    return new PartitionCollection(collection, Lists.newArrayList(), this);
  }

  private ResourceOptions prepareOptions() {
    return ResourceSupport.options(APPLICATION_JSON)
        .tokenProvider(client.resourceTokenProvider());
  }

  private UriBuilder collectionUri() {
    return UriBuilder.builder(client.baseURI()).path(PATH_EVENT_TYPES);
  }
}
