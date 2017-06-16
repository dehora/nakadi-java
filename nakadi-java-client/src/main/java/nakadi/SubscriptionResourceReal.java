package nakadi;

import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubscriptionResourceReal implements SubscriptionResource {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final String PATH_CURSORS = "cursors";
  private static final String PATH_STATS = "stats";
  private static final String PATH = "subscriptions";
  private static final String APPLICATION_JSON = "application/json";
  private static final Type TYPE_CURSOR_COMMIT_RESULT =
      new TypeToken<CursorCommitResultCollection>() {
      }.getType();
  private static final Type TYPE = new TypeToken<List<Subscription>>() {
  }.getType();

  private final NakadiClient client;
  private CursorCommitResultCollection sentinelCursorCommitResultCollection;
  private String scope;
  private volatile RetryPolicy retryPolicy;

  SubscriptionResourceReal(NakadiClient client) {
    this.client = client;
    this.sentinelCursorCommitResultCollection =
        new CursorCommitResultCollection(new ArrayList<>(), new ArrayList<>(), this, client);
  }

  private static Response timed(Supplier<Response> sender, NakadiClient client,
      MetricCollector.Timer timer) {
    final long start = System.nanoTime();
    try {
      return sender.get();
    } finally {
      try {
        client.metricCollector().duration(timer, (System.nanoTime() - start), TimeUnit.NANOSECONDS);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

  @Override public SubscriptionResource scope(String scope) {
    this.scope = scope;
    return this;
  }

  @Override public SubscriptionResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  @Override public Response createReturningResponse(Subscription subscription)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    //todo:filebug: nakadi.event_stream.read is in the yaml but this is a write action
    NakadiException.throwNonNull(subscription, "Please provide a subscription");
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.POST, collectionUri().buildString(),
            options, () -> client.jsonSupport().toJsonBytes(subscription));
  }

  @Override public Subscription create(Subscription subscription)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, ConflictException, NakadiException {
    NakadiException.throwNonNull(subscription, "Please provide a subscription");
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.POST, collectionUri().buildString(),
            options, () -> client.jsonSupport().toJsonBytes(subscription), Subscription.class);
  }

  @Override public Subscription find(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    NakadiException.throwNonNull(id, "Please provide an id");
    String url = collectionUri().path(id).buildString();
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options, Subscription.class);
  }

  @Override public Optional<Subscription> tryFind(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    try {
      return Optional.of(find(id));
    } catch (NotFoundException e) {
      return Optional.empty();
    }
  }

  @Override public SubscriptionCollection list()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    return list(new QueryParams());
  }

  @Override public SubscriptionCollection list(QueryParams params)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    NakadiException.throwNonNull(params, "Please provide query params");
    return loadPage(collectionUri().query(params).buildString());
  }

  @Override public Response delete(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    NakadiException.throwNonNull(id, "Please provide an id");
    String url = collectionUri().path(id).buildString();
    // todo:filebug: no delete operation in yaml, got with config write as per event type delete
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_CONFIG_WRITE);
    return client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.DELETE, url, options);
  }

  @Override
  public CursorCommitResultCollection checkpoint(Map<String, String> context, Cursor... cursors)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, ContractException, NakadiException {

    return checkpoint(retryPolicy, context, cursors);
  }

  @Override public Response reset(String id, List<Cursor> cursors)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, ConflictException, NakadiException {
    NakadiException.throwNonNull(id, "Please provide an id");
    NakadiException.throwNonNull(cursors, "Please provide a cursors list");
    return reset(retryPolicy, id, cursors);
  }

  @Override public SubscriptionCursorCollection cursors(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    NakadiException.throwNonNull(id, "Please provide an id");
    return loadCursorPage(collectionUri().path(id).path(PATH_CURSORS).buildString());
  }

  @Override public SubscriptionEventTypeStatsCollection stats(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    NakadiException.throwNonNull(id, "Please provide an id");
    return loadStatsPage(collectionUri().path(id).path(PATH_STATS).buildString());
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  CursorCommitResultCollection checkpoint(RetryPolicy backoff, Map<String, String> context,
      Cursor... cursors) {
    NakadiException.throwNonNull(cursors, "Please provide cursors");
    NakadiException.throwNonNull(context, "Please provide a context map");
    NakadiException.throwNonNull(context.get(StreamResourceSupport.X_NAKADI_STREAM_ID),
        "Please provide the header " + StreamResourceSupport.X_NAKADI_STREAM_ID);
    NakadiException.throwNonNull(context.get(StreamResourceSupport.SUBSCRIPTION_ID),
        "Please provide the subscription id");

    List<Cursor> cursorList = Arrays.asList(cursors);

    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put("items", cursorList);

    String streamId = context.get(StreamResourceSupport.X_NAKADI_STREAM_ID);
    String subscriptionId = context.get(StreamResourceSupport.SUBSCRIPTION_ID);
    String url = collectionUri()
        .path(subscriptionId)
        .path(SubscriptionResourceReal.PATH_CURSORS)
        .buildString();

    // todo:filebug: 'nakadi.event_stream.read' in yaml but this is a write method
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);

    options.header(StreamResourceSupport.X_NAKADI_STREAM_ID, streamId);

    Resource resource = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy);

    Response response;
    final MetricCollector.Timer timer = MetricCollector.Timer.checkpointSend;

    response = timed(() -> resource.requestThrowing(Resource.POST, url, options,
        () -> client.jsonSupport().toJsonBytes(requestMap)), client, timer);

    if (response.statusCode() == 204) {
      return sentinelCursorCommitResultCollection;
    }

    if (response.statusCode() == 200) {
      String raw = response.responseBody().asString();
      return client.jsonSupport()
          .fromJson(raw, TYPE_CURSOR_COMMIT_RESULT);
    }

    // success but not expected, throw this to signal
    throw new ContractException(Problem.contractProblem(
        "Success committing cursor with unexpected code", "response: " + response));
  }

  SubscriptionEventTypeStatsCollection loadStatsPage(String url) {
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    /*
    map response to the local collection api; this allows iterators and iterables to be used
    over the results. the api doesn't page, so pass an empty list for pagination
     */
    SubscriptionEventTypeStatsList cursors =
        client.jsonSupport()
            .fromJson(response.responseBody().asString(), SubscriptionEventTypeStatsList.class);
    List<SubscriptionEventTypeStats> items = cursors.items();
    return new SubscriptionEventTypeStatsCollection(items, new ArrayList<>(), this, client);
  }

  SubscriptionCursorCollection loadCursorPage(String url) {
    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    /*
    map response to the local collection api; this allows iterators and iterables to be used
    over the results. the api doesn't page, so pass an empty list for pagination
     */
    SubscriptionCursorList cursors =
        client.jsonSupport()
            .fromJson(response.responseBody().asString(), SubscriptionCursorList.class);
    List<Cursor> items = cursors.items();
    return new SubscriptionCursorCollection(items, new ArrayList<>(), this, client);
  }

  SubscriptionCollection loadPage(String url) {
    ResourceOptions options = this.prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    /*
    map server response to the local collection api; the server yaml doesn't define a direct
    response object just a pair of PaginationLinks and List<Subscription> items, so we
    define and use a temp SubscriptionList to capture it. We also remap PaginationLinks because
    they don't capture rels (this is a problem with HAL, the underlying format used for links).

    todo: replace SubscriptionList entirely as we're dropping unexpected rels via PaginationLinks
     */
    SubscriptionList list =
        client.jsonSupport().fromJson(response.responseBody().asString(), SubscriptionList.class);
    return new SubscriptionCollection(toSubscriptions(list.items()), toLinks(list._links()), this, client);
  }

  private Response reset(RetryPolicy retryPolicy, String id, List<Cursor> cursors) {
    final Resource resource = client.resourceProvider().newResource().retryPolicy(retryPolicy);
    final String url = collectionUri().path(id).path(PATH_CURSORS).buildString();
    // read scope: see https://github.com/zalando/nakadi/issues/648
    final ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    final List<Cursor> cleaned = Cursor.prepareRequiringEventType(cursors);

    return timed(() ->
            resource.requestThrowing(
                Resource.PATCH,
                url,
                options,
                () -> client.jsonSupport().toJsonBytes(new CursorResetCollection(cleaned))
            ),
        client,
        MetricCollector.Timer.cursorReset);
  }

  private List<ResourceLink> toLinks(PaginationLinks _links) {
    return new LinkSupport().toLinks(_links);
  }

  private List<Subscription> toSubscriptions(List<Subscription> items) {
    List<Subscription> subscriptions = new ArrayList<>();
    if (items != null) {
      subscriptions.addAll(items);
    }
    return subscriptions;
  }

  private ResourceOptions prepareOptions(String fallbackScope) {
    return ResourceSupport.options(APPLICATION_JSON)
        .scope(Optional.ofNullable(scope).orElse(fallbackScope))
        .tokenProvider(client.resourceTokenProvider());
  }

  private UriBuilder collectionUri() {
    return UriBuilder.builder(client.baseURI()).path(PATH);
  }

  ResourceCollection<CursorCommitResult> loadCursorCommitPage(String url) {
    return new CursorCommitResultCollection(loadCollection(url), new ArrayList<>(), this, client);
  }

  private List<CursorCommitResult> loadCollection(String url) {

    ResourceOptions options = prepareOptions(TokenProvider.NAKADI_EVENT_STREAM_READ);
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, options);

    return client.jsonSupport().fromJson(response.responseBody().asString(), TYPE);
  }

  // todo: decide how this gets handling/set within a stream
  private RetryPolicy policyBackoffForCheckpoint() {
    return ExponentialRetry.newBuilder()
        .initialInterval(900, TimeUnit.MILLISECONDS)
        .maxAttempts(3)
        .maxInterval(2000, TimeUnit.MILLISECONDS)
        .build();
  }

  @VisibleForTesting
  static class CursorResetCollection {

    final List<Cursor> items;

    CursorResetCollection(List<Cursor> items) {
      this.items = items;
    }
  }

  private static class SubscriptionList {

    private final PaginationLinks _links = null;
    private final List<Subscription> items = new ArrayList<>();

    PaginationLinks _links() {
      return _links;
    }

    List<Subscription> items() {
      return items;
    }
  }

  private static class SubscriptionCursorList {

    private final List<Cursor> items = new ArrayList<>();

    List<Cursor> items() {
      return items;
    }
  }

  private static class SubscriptionEventTypeStatsList {

    private final List<SubscriptionEventTypeStats> items = new ArrayList<>();

    List<SubscriptionEventTypeStats> items() {
      return items;
    }
  }
}
