package nakadi;

import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Supports API operations related to metrics.
 */
public class MetricsResource {

  public static final String PATH = "metrics";
  private static final String APPLICATION_JSON = "application/json";
  private static final Type TYPE = new TypeToken<Map<String, Object>>() {
  }.getType();

  private final NakadiClient client;
  private volatile RetryPolicy retryPolicy;

  public MetricsResource(NakadiClient client) {
    this.client = client;
  }

  public MetricsResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  /**
   * Fetch server metrics.
   *
   * @return server metrics.
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  public Metrics get()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    String url = UriBuilder.builder(client.baseURI()).path(PATH).buildString();
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(retryPolicy)
        .requestThrowing(Resource.GET, url, prepareOptions());

    // shift response data into metric items to make it accessible
    Map<String, Object> items =
        client.jsonSupport().fromJson(response.responseBody().asString(), TYPE);
    return new Metrics().items(items);
  }

  private ResourceOptions prepareOptions() {
    return ResourceSupport.options(APPLICATION_JSON).tokenProvider(client.resourceTokenProvider());
  }
}
