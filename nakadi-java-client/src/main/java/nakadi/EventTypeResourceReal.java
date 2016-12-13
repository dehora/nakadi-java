package nakadi;

import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class EventTypeResourceReal implements EventTypeResource {

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_PARTITIONS = "partitions";
  private static final String PATH_SCHEMAS = "schemas";
  private static final String APPLICATION_JSON = "application/json";
  private static final Type TYPE = new TypeToken<List<EventType>>() {
  }.getType();
  private static final Type TYPE_P = new TypeToken<List<Partition>>() {
  }.getType();
  private static final Type TYPE_ETS = new TypeToken<List<EventTypeSchema>>() {
  }.getType();

  private final NakadiClient client;
  private String scope;
  private volatile RetryPolicy retryPolicy;

  EventTypeResourceReal(NakadiClient client) {
    this.client = client;
  }

  @Override public EventTypeResource scope(String scope) {
    this.scope = scope;
    return this;
  }

  @Override public EventTypeResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  @Override public Response create(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {

    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_TYPE_WRITE);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.POST, collectionUri().buildString(), options, eventType);
  }

  @Override public Response update(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventType.name()).buildString();
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_TYPE_WRITE);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.PUT, url, options, eventType);
  }

  @Override public EventType findByName(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventTypeName).buildString();
    // filebug: no scope defined on this resource; work with NAKADI_EVENT_STREAM_READ for now
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options, EventType.class);
  }

  @Override public Optional<EventType> tryFindByName(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, RateLimitException,
      NakadiException {
    try {
      return Optional.of(findByName(eventTypeName));
    } catch (NotFoundException e) {
      return Optional.empty();
    }
  }

  @Override public Response delete(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventTypeName).buildString();
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_CONFIG_WRITE);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.DELETE, url, options);
  }

  @Override public EventTypeCollection list()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    return loadPage(collectionUri().buildString());
  }

  @Override public PartitionCollection partitions(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    return loadPartitionPage(
        collectionUri().path(eventTypeName).path(PATH_PARTITIONS).buildString());
  }

  @Override public Partition partition(String eventTypeName, String partitionId)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    final String url =
        collectionUri().path(eventTypeName).path(PATH_PARTITIONS).path(partitionId).buildString();
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options, Partition.class);
  }

  @Override public EventTypeSchemaCollection schemas(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException,
      RateLimitException, NakadiException {
    return loadSchemaPage(
        collectionUri().path(eventTypeName).path(PATH_SCHEMAS).buildString());
  }

  String applyScope(String fallbackScope) {
    return scope == null ? fallbackScope: scope;
  }

  EventTypeCollection loadPage(String url) {
    // filebug: no scope defined on this resource; work with NAKADI_EVENT_STREAM_READ for now
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    List<EventType> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE);

    return new EventTypeCollection(collection, new ArrayList<>(), this);
  }

  PartitionCollection loadPartitionPage(String url) {
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    List<Partition> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE_P);

    return new PartitionCollection(collection, new ArrayList<>(), this);
  }

  EventTypeSchemaCollection loadSchemaPage(String url) {
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    EventTypeSchemaList list =
        client.jsonSupport()
            .fromJson(response.responseBody().asString(), EventTypeSchemaList.class);

    return new EventTypeSchemaCollection(
        toEventTypeSchema(list.items()),
        toLinks(list._links()),
        this);
  }

  private ResourceOptions prepareOptions(String fallbackScope) {
    return ResourceSupport.options(APPLICATION_JSON)
        .tokenProvider(client.resourceTokenProvider())
        .scope(applyScope(fallbackScope)); // use the set scope or fallback
  }

  private UriBuilder collectionUri() {
    return UriBuilder.builder(client.baseURI()).path(PATH_EVENT_TYPES);
  }

  private List<ResourceLink> toLinks(PaginationLinks _links) {
    List<ResourceLink> links = new ArrayList<>();
    if (_links != null) {
      final PaginationLink prev = _links.prev();
      final PaginationLink next = _links.next();

      if (prev != null) {
        links.add(new ResourceLink("prev", prev.href()));
      }

      if (next != null) {
        links.add(new ResourceLink("next", next.href()));
      }
    }
    return links;
  }

  private List<EventTypeSchema> toEventTypeSchema(List<EventTypeSchema> items) {
    List<EventTypeSchema> subscriptions = new ArrayList<>();
    if (items != null) {
      subscriptions.addAll(items);
    }
    return subscriptions;
  }

  private static class EventTypeSchemaList {

    private final PaginationLinks _links = null;
    private final List<EventTypeSchema> items = new ArrayList<>();

    PaginationLinks _links() {
      return _links;
    }

    List<EventTypeSchema> items() {
      return items;
    }
  }


}
