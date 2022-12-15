package nakadi;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class EventTypeResourceReal implements EventTypeResource {

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_PARTITIONS = "partitions";
  private static final String PATH_SCHEMAS = "schemas";
  private static final String PATH_CURSOR_SHIFTS = "shifted-cursors";
  private static final String PATH_CURSOR_DISTANCE = "cursor-distances";
  private static final String APPLICATION_JSON = "application/json";
  private static final Type TYPE = new TypeToken<List<EventType>>() {
  }.getType();
  private static final Type TYPE_P = new TypeToken<List<Partition>>() {
  }.getType();
  private static final Type TYPE_ETS = new TypeToken<List<EventTypeSchema>>() {
  }.getType();
  private static final Type TYPE_C = new TypeToken<List<Cursor>>() {
  }.getType();
  private static final Type TYPE_CD = new TypeToken<List<CursorDistance>>() {
  }.getType();
  private static final List<ResourceLink> SENTINEL_LINKS = Collections.emptyList();

  private final NakadiClient client;
  private volatile RetryPolicy retryPolicy;

  EventTypeResourceReal(NakadiClient client) {
    this.client = client;
  }

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0. Scopes set here are ignored.
   *
   * @return this
   */
  @Deprecated
  @Override public EventTypeResource scope(String scope) {
    return this;
  }

  @Override public EventTypeResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  @Override public Response create(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {

    // todo: close
    ResourceOptions options = ResourceSupport.optionsWithJsonContent(prepareOptions());
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.POST, collectionUri().buildString(), options,
            () -> client.jsonSupport().toJsonBytes(eventType));
  }

  @Override public Response update(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventType.name()).buildString();
    ResourceOptions options = ResourceSupport.optionsWithJsonContent(prepareOptions());
    // todo: close
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.PUT, url, options,
            () -> client.jsonSupport().toJsonBytes(eventType));
  }

  @Override public EventType findByName(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = collectionUri().path(eventTypeName).buildString();
    ResourceOptions options = prepareOptions();
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
    ResourceOptions options = prepareOptions();
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
    ResourceOptions options = prepareOptions();
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options, Partition.class);
  }

  @Override public Partition partition(String eventTypeName, String partitionId, QueryParams params)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    final String url =
        collectionUri().path(eventTypeName)
            .path(PATH_PARTITIONS)
            .path(partitionId)
            .query(params)
            .buildString();
    ResourceOptions options = prepareOptions();
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

  @Override public CursorCollection shift(String eventTypeName, List<Cursor> cursorList) {
    NakadiException.throwNotNullOrEmpty(cursorList, "Please provide at least one cursor");

    final String url = collectionUri().path(eventTypeName).path(PATH_CURSOR_SHIFTS).buildString();
    final ResourceOptions options = ResourceSupport.optionsWithJsonContent(prepareOptions());
    final Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.POST, url, options,
            () -> client.jsonSupport().toJsonBytes(cursorList));

    final List<Cursor> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE_C);

    return new CursorCollection(collection, SENTINEL_LINKS, client);
  }

  @Override
  public CursorDistanceCollection distance(
      String eventTypeName, List<CursorDistance> cursorDistanceList) {

    final String url = collectionUri().path(eventTypeName).path(PATH_CURSOR_DISTANCE).buildString();
    final ResourceOptions options = ResourceSupport.optionsWithJsonContent(prepareOptions());
    final Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.POST, url, options,
            () -> client.jsonSupport().toJsonBytes(cursorDistanceList));

    final List<CursorDistance> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE_CD);

    return new CursorDistanceCollection(collection, SENTINEL_LINKS, client);
  }

  @Override public PartitionCollection lag(String eventTypeName, List<Cursor> cursors) {
    final String url = collectionUri().path(eventTypeName).path("cursors-lag").buildString();
    final ResourceOptions options = ResourceSupport.optionsWithJsonContent(prepareOptions());

    final Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.POST, url, options,
            () -> client.jsonSupport().toJsonBytes(cursors));

    final List<Partition> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE_P);

    return new PartitionCollection(collection, SENTINEL_LINKS, this, client);
  }

  EventTypeCollection loadPage(String url) {
    // filebug: no scope defined on this resource; work with NAKADI_EVENT_STREAM_READ for now
    ResourceOptions options = prepareOptions();
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    List<EventType> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE);

    return new EventTypeCollection(collection, new ArrayList<>(), this, client);
  }

  PartitionCollection loadPartitionPage(String url) {
    ResourceOptions options = prepareOptions();
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    List<Partition> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE_P);

    return new PartitionCollection(collection, new ArrayList<>(), this, client);
  }

  EventTypeSchemaCollection loadSchemaPage(String url) {
    ResourceOptions options = prepareOptions();
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
        this,
        client);
  }

  private ResourceOptions prepareOptions() {
    return ResourceSupport.options(APPLICATION_JSON)
        .tokenProvider(client.resourceTokenProvider());
  }

  private UriBuilder collectionUri() {
    return UriBuilder.builder(client.baseURI()).path(PATH_EVENT_TYPES);
  }

  private List<ResourceLink> toLinks(PaginationLinks _links) {
    return new LinkSupport().toLinks(_links);
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
