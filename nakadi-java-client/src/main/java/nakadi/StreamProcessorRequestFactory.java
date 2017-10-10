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
    final String mode = sc.isEventTypeStream() ? "eventStream" : "subscriptionStream";
    logger.debug("stream_connection_open step=request mode={} url={}", mode, url);

    final Response response = requestStreamConnection(url, options, buildResource(sc));

    streamProcessor.currentStreamResponseCode(response.statusCode());

    logger.info("stream_connection_open step=response hash={} response={} x_nakadi_stream_id={} ", response.hashCode(), response, response.headers().get(StreamProcessor.X_NAKADI_STREAM_ID));

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
