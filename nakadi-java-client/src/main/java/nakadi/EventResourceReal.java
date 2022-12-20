package nakadi;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventResourceReal implements EventResource {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_COLLECTION = "events";
  private static final String APPLICATION_JSON = "application/json";

  private static final List<ResourceLink> LINKS_SENTINEL = Lists.newArrayList();
  private static final Map<String, Object> SENTINEL_HEADERS = Maps.newHashMap();
  private static Type TYPE_BIR = new TypeToken<List<BatchItemResponse>>() {
  }.getType();

  private final NakadiClient client;
  private final JsonSupport jsonSupport;
  private volatile RetryPolicy retryPolicy;
  private volatile String flowId;
  private boolean enablePublishingCompression;

  private final CompressionSupport compressionSupport;
  private final SerializationSupport serializationSupport;

  public EventResourceReal(NakadiClient client) {
    this(client, client.jsonSupport(), client.compressionSupport(), client.getSerializationSupport());
  }

  @VisibleForTesting
  EventResourceReal(NakadiClient client,
                    JsonSupport jsonSupport,
                    CompressionSupport compressionSupport,
                    SerializationSupport serializationSupport) {
    this.client = client;
    this.jsonSupport = jsonSupport;
    this.compressionSupport = compressionSupport;
    if(client != null && client.enablePublishingCompression()) {
      this.enablePublishingCompression = true;
    }
    this.serializationSupport = serializationSupport;
  }

  private static Response timed(Supplier<Response> sender, NakadiClient client, int eventCount) {
    long start = System.nanoTime();
    Response response = null;
    try {
      response = sender.get();
      return response;
    } finally {
      if (response != null) {
        emitMetric(client, response, eventCount);
      }
      client.metricCollector().duration(
          MetricCollector.Timer.eventSend, (System.nanoTime() - start), TimeUnit.NANOSECONDS);
    }
  }

  private static void emitMetric(NakadiClient client, Response response, int eventCount) {
    if (response.statusCode() >= 200 && response.statusCode() <= 204) {
      client.metricCollector().mark(MetricCollector.Meter.sent, eventCount);
    }

    if (response.statusCode() == 207) {
      client.metricCollector().mark(MetricCollector.Meter.http207);
    }
  }

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0. Scopes set here are ignored.
   *
   * @param scope the OAuth scope to be used for the request
   * @return this
   */
  @Deprecated
  @Override public EventResource scope(String scope) {
    return this;
  }

  @Override public EventResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  @Override public EventResource flowId(final String flowId) {
    this.flowId = flowId;
    return this;
  }

  @Override
  public final <T> Response send(String eventTypeName, Collection<T> events) {
    return send(eventTypeName,events, SENTINEL_HEADERS);
  }

  @Override public <T> Response send(String eventTypeName, Collection<T> events,
      Map<String, Object> headers) {
    NakadiException.throwNonNull(eventTypeName, "Please provide an event type name");
    NakadiException.throwNonNull(events, "Please provide one or more events");
    NakadiException.throwNonNull(headers, "Please provide some headers");

    if (events.isEmpty()) {
      throw new NakadiException(Problem.localProblem("event send called with zero events", ""));
    }

    if (new ArrayList<>(events).get(0) instanceof String) {
      return sendUsingSupplier(eventTypeName,
          () -> ("[" + Joiner.on(",").join(events) + "]").getBytes(Charsets.UTF_8), headers);
    } else {
      return sendBatchOfEvents(eventTypeName, events, headers);
    }
  }

  @Override
  public <T> Response send(String eventTypeName, T event) {
    return send(eventTypeName, event, SENTINEL_HEADERS);
  }

  @Override public <T> Response send(String eventTypeName, T event, Map<String, Object> headers) {
    NakadiException.throwNonNull(eventTypeName, "Please provide an event type name");
    NakadiException.throwNonNull(event, "Please provide an event");
    NakadiException.throwNonNull(headers, "Please provide some headers");

    if (event instanceof String) {

      ContentSupplier supplier;

      if(enablePublishingCompression) {
        supplier =  supplyStringAsCompressedAndSetHeaders("[" + event + "]", headers);
      } else {
        supplier = () -> ("[" + event + "]").getBytes(Charsets.UTF_8);
      }

      final ContentSupplier finalSupplier = supplier;
      return sendUsingSupplier(eventTypeName, finalSupplier, headers);

    } else {
      ArrayList<T> events = new ArrayList<>(1);
      Collections.addAll(events, event);
      return send(eventTypeName, events);
    }
  }

  @Override public <T> BatchItemResponseCollection sendBatch(String eventTypeName, List<T> events) {
    return sendBatch(eventTypeName, events, SENTINEL_HEADERS);
  }

  @Override public <T> BatchItemResponseCollection sendBatch(String eventTypeName, List<T> events,
      Map<String, Object> headers) {

    List<BatchItemResponse> items = Lists.newArrayList();
    try (Response send = send(eventTypeName, events, headers)) {
      if (send.statusCode() == 207 || send.statusCode() == 422) {
        ResponseBody responseBody = send.responseBody();
        items.addAll(jsonSupport.fromJson(responseBody.asReader(), TYPE_BIR));
      }
    }
    return new BatchItemResponseCollection(items, LINKS_SENTINEL, client);
  }

  private Response sendUsingSupplier(String eventTypeName, ContentSupplier supplier,
      Map<String, Object> headers) {
    // todo: close
    return timed(() -> client.resourceProvider()
                     .newResource()
                     .retryPolicy(retryPolicy)
                     .postEventsThrowing(
                         collectionUri(eventTypeName).buildString(), options(headers), supplier),
                 client,
                 1);
  }

  private <T> Response sendBatchOfEvents(String eventTypeName, Collection<T> events, Map<String, Object> headers) {
    NakadiException.throwNonNull(events, "Please provide one or more event records");

    final ContentSupplier supplier;

    if(enablePublishingCompression) {
      supplier =  supplyObjectAsCompressedAndSetHeaders(eventTypeName, events, headers);
    } else {
      supplier = () -> serializationSupport.serializePayload(client, eventTypeName, events);
    }

    // todo: close
    return timed(() -> client.resourceProvider()
                     .newResource()
                     .retryPolicy(retryPolicy)
                     .postEventsThrowing(
                         collectionUri(eventTypeName).buildString(),
                         options(headers),
                             supplier),
              client,
        events.size());
  }

  private ResourceOptions options(Map<String, Object> headers) {
      final ResourceOptions options = ResourceSupport.options(APPLICATION_JSON);
      options.tokenProvider(client.resourceTokenProvider());
      if (flowId != null) {
          options.flowId(flowId);
      }
      options.headers(headers);
      options.header(ResourceOptions.HEADER_CONTENT_TYPE, serializationSupport.contentType());
      return options;
  }

  private UriBuilder collectionUri(String topic) {
    return UriBuilder.builder(client.baseURI())
        .path(PATH_EVENT_TYPES)
        .path(topic)
        .path(PATH_COLLECTION);
  }

  private <T> ContentSupplier supplyObjectAsCompressedAndSetHeaders(String eventTypeName, Collection<T> sending, Map<String, Object> headers) {
    final byte[] batchBytes = serializationSupport.serializePayload(client, eventTypeName, sending);
    return supplyBytesAsCompressedAndSetHeaders(batchBytes, headers);
  }

  private <T> ContentSupplier supplyStringAsCompressedAndSetHeaders(String sending, Map<String, Object> headers) {
    /*
    Minifying this successfully would require marshalling up to an object
    and back to a string. To avoid that overhead, compress the string as is.
     */
    final byte[] json = sending.getBytes(Charsets.UTF_8);
    return supplyBytesAsCompressedAndSetHeaders(json, headers);
  }

  private ContentSupplier supplyBytesAsCompressedAndSetHeaders(
      byte[] batchBytes, Map<String, Object> headers) {

    // force the compression outside the lambda to access the length
    final byte[] compressed = compressionSupport.compress(batchBytes);
    ContentSupplier supplier = () -> compressed;
    headers.put("Content-Length", compressed.length);
    headers.put("Content-Encoding", compressionSupport.name());
    return supplier;
  }
}
