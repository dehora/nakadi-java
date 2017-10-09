package nakadi;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamProcessorRequestFactory {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final NakadiClient client;

  StreamProcessorRequestFactory(NakadiClient client) {
    this.client = client;
  }

  Callable<Response> createCallable(StreamConfiguration sc, StreamProcessor streamProcessor) {
    return () -> onCall(sc, streamProcessor);
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Response onCall(StreamConfiguration sc, StreamProcessor streamProcessor) throws Exception {
    final String url = StreamResourceSupport.buildStreamUrl(client.baseURI(), sc);
    ResourceOptions options = StreamResourceSupport.buildResourceOptions(client, sc);
    logger.info("stream_connection_open step=details mode={} url={}",
        sc.isEventTypeStream() ? "eventStream" : "subscriptionStream",
        url);

    final Response response = requestStreamConnection(url, options, buildResource(sc));

    streamProcessor.currentStreamResponseCode(response.statusCode());

    logger.info("stream_connection_open step=opened {} {}", response.hashCode(), response);

    return response;
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Response requestStreamConnection(String url, ResourceOptions options, Resource resource) {
    return resource.requestThrowing(Resource.GET, url, options);
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Resource buildResource(StreamConfiguration sc) {
    return client.resourceProvider()
        .newResource()
        .readTimeout(sc.readTimeoutMillis(), TimeUnit.MILLISECONDS)
        .connectTimeout(sc.connectTimeoutMillis(), TimeUnit.MILLISECONDS);
  }

}
