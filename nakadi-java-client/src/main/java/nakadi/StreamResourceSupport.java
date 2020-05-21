package nakadi;

import java.net.URI;
import java.util.List;
import java.util.Optional;

/**
 * Configure the URL and headers used to consume batches from the server.
 */
class StreamResourceSupport {

  public static final String X_NAKADI_STREAM_ID = "X-Nakadi-StreamId";
  public static final String SUBSCRIPTION_ID = "SubscriptionId";
  private static final String PARAM_BATCH_LIMIT = "batch_limit";
  private static final String PARAM_STREAM_LIMIT = "stream_limit";
  private static final String PARAM_BATCH_FLUSH_TIMEOUT = "batch_flush_timeout";
  private static final String PARAM_STREAM_TIMEOUT = "stream_timeout";
  private static final String PARAM_COMMIT_TIMEOUT = "commit_timeout";
  private static final String PARAM_STREAM_KEEP_ALIVE_LIMIT = "stream_keep_alive_limit";
  private static final String PARAM_MAX_UNCOMMITTED_EVENTS = "max_uncommitted_events";
  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_SUBS = "subscriptions";
  private static final String PATH_EVENTS = "events";
  private static final String APPLICATION_X_JSON_STREAM = "application/x-json-stream";
  private static final String APPLICATION_JSON = "application/json";
  private static final String HEADER_X_NAKADI_CURSORS = "X-Nakadi-Cursors";

  static String buildStreamUrl(URI baseUri, StreamConfiguration sc) {

    UriBuilder uriBuilder = UriBuilder.builder(baseUri);

    if (sc.isSubscriptionStream()) {
      uriBuilder.path(PATH_SUBS).path(sc.subscriptionId()).path(PATH_EVENTS);
      uriBuilder = applySubscriptionQueryParamsTo(uriBuilder, sc);
    } else {
      uriBuilder.path(PATH_EVENT_TYPES).path(sc.eventTypeName()).path(PATH_EVENTS);
      uriBuilder = applyParamsTo(uriBuilder, sc);
    }

    return uriBuilder.buildString();
  }

  /**
   * Configure the header for cursors. The X-Nakadi-Cursors headers is set with the
   * single line JSON encoding of {@link StreamConfiguration#cursors()} if the configuration
   * is a basic event stream.
   */
  static ResourceOptions buildResourceOptions(NakadiClient client, StreamConfiguration sc) {
    ResourceOptions options = ResourceSupport
        // breaks with api definition https://github.com/zalando-incubator/nakadi-java/issues/98
        .options(APPLICATION_JSON)
        .tokenProvider(client.resourceTokenProvider());

    applyConfiguredHeaders(sc, options);

    if (sc.isSubscriptionStream()) {
      return options;
    }

    Optional<List<Cursor>> cursors = sc.cursors(); // deref to keep stop idea complaining
    cursors.ifPresent(
        list ->
            options.header(HEADER_X_NAKADI_CURSORS, client.jsonSupport().toJsonCompressed(list)));
    return options;
  }

  private static void applyConfiguredHeaders(StreamConfiguration sc, ResourceOptions options) {
    sc.requestHeaders().forEach(options::header);
  }

  /**
   * Configure the url with params:
   *
   * <ul>
   * <li>batch_limit: {@link StreamConfiguration#batchLimit()}</li>
   * <li>stream_limit: {@link StreamConfiguration#streamLimit()}</li>
   * <li>batch_flush_timeout: {@link StreamConfiguration#batchFlushTimeoutSeconds()}</li>
   * <li>stream_timeout: {@link StreamConfiguration#streamTimeoutSeconds()}</li>
   * <li>stream_keep_alive_limit: {@link StreamConfiguration#streamKeepAliveLimit()}</li>
   * <li>commit_timeout: {@link StreamConfiguration#commitTimeoutSeconds()}</li>
   * </ul>
   *
   * Only values not matching the defaults are set.
   */
  private static UriBuilder applyParamsTo(UriBuilder uriBuilder, StreamConfiguration sc) {

    // ignores 0 or lower values: see https://github.com/zalando-incubator/nakadi-java/issues/125
    if (sc.batchLimit() > StreamConfiguration.DEFAULT_BATCH_LIMIT) {
      uriBuilder.query(PARAM_BATCH_LIMIT, "" + sc.batchLimit());
    }

    if (sc.streamLimit() != StreamConfiguration.DEFAULT_STREAM_LIMIT) {
      uriBuilder.query(PARAM_STREAM_LIMIT, "" + sc.streamLimit());
    }

    if (sc.batchFlushTimeoutSeconds() != StreamConfiguration.DEFAULT_BATCH_FLUSH_TIMEOUT) {
      uriBuilder.query(PARAM_BATCH_FLUSH_TIMEOUT, "" + sc.batchFlushTimeoutSeconds());
    }

    if (sc.streamTimeoutSeconds() != StreamConfiguration.DEFAULT_STREAM_TIMEOUT) {
      uriBuilder.query(PARAM_STREAM_TIMEOUT, "" + sc.streamTimeoutSeconds());
    }

    if (sc.streamKeepAliveLimit() != StreamConfiguration.DEFAULT_STREAM_KEEPALIVE_COUNT) {
      uriBuilder.query(PARAM_STREAM_KEEP_ALIVE_LIMIT, "" + sc.streamKeepAliveLimit());
    }

    if (sc.commitTimeoutSeconds() != StreamConfiguration.DEFAULT_COMMIT_TIMEOUT) {
      uriBuilder.query(PARAM_COMMIT_TIMEOUT, "" + sc.commitTimeoutSeconds());
    }

    return uriBuilder;
  }

  private static UriBuilder applySubscriptionQueryParamsTo(UriBuilder uriBuilder,
      StreamConfiguration sc) {

    if (sc.maxUncommittedEvents() != StreamConfiguration.DEFAULT_MAX_UNCOMMITTED_EVENTS) {
      uriBuilder.query(PARAM_MAX_UNCOMMITTED_EVENTS, "" + sc.maxUncommittedEvents());
    }

    return applyParamsTo(uriBuilder, sc);
  }
}
