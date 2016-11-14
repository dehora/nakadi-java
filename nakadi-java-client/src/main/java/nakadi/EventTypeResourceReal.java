package nakadi;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.TimeUnit;

class EventTypeResourceReal implements EventTypeResource {

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_PARTITIONS = "partitions";
  private static final String APPLICATION_JSON = "application/json";
  private static final Type TYPE = new TypeToken<List<EventType>>() {
  }.getType();
  private static final Type TYPE_P = new TypeToken<List<Partition>>() {
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

    return new EventTypeCollection(collection, Lists.newArrayList(), this);
  }

  PartitionCollection loadPartitionPage(String url) {
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    List<Partition> collection =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE_P);

    return new PartitionCollection(collection, Lists.newArrayList(), this);
  }

  private ResourceOptions prepareOptions(String fallbackScope) {
    return ResourceSupport.options(APPLICATION_JSON)
        .tokenProvider(client.resourceTokenProvider())
        .scope(applyScope(fallbackScope)); // use the set scope or fallback
  }

  private UriBuilder collectionUri() {
    return UriBuilder.builder(client.baseURI()).path(PATH_EVENT_TYPES);
  }

}
