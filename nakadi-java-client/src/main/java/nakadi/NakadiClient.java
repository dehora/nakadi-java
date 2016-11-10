package nakadi;

import com.google.common.collect.Lists;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client which can be used to work with the server. Clients must be created via a builder.
 * Once built, callers can access the server via the resources available from the
 * {@link #resources()} method.
 */
public class NakadiClient {

  static final String PLATFORM_DETAILS_JSON = GsonSupport.gsonCompressed()
      .toJson(Lists.newArrayList(
          "os.arch", "os.name", "os.version", "java.class.version", "java.runtime.version",
          "java.vm.name", "java.vm.version"
      ).stream().collect(Collectors.toMap(String::toString, System::getProperty)));
  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final String VERSION = Version.VERSION;
  static final String USER_AGENT = "nakadi-java/" + NakadiClient.VERSION;
  private final URI baseURI;
  private final JsonSupport jsonSupport;
  private final ResourceProvider resourceProvider;
  private final ResourceTokenProvider resourceTokenProvider;
  private final MetricCollector metricCollector;
  private final Resources resources;

  private NakadiClient(Builder builder) {
    NakadiException.throwNonNull(builder.baseURI, "Please provide a base URI.");
    this.baseURI = builder.baseURI;
    this.jsonSupport = builder.jsonSupport;
    this.resourceProvider = builder.resourceProvider;
    this.resourceTokenProvider = builder.resourceTokenProvider;
    this.metricCollector = builder.metricCollector;
    this.resources = new Resources(this);
  }

  /**
   * Get a builder that can construct a new client. Clients can't be created directly.
   *
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * The base API url the client is using.
   */
  public URI baseURI() {
    return baseURI;
  }

  /**
   * The {@link ResourceProvider} used by the client.
   */
  public ResourceProvider resourceProvider() {
    return resourceProvider;
  }

  /**
   * The {@link ResourceTokenProvider} used by the client.
   */
  public ResourceTokenProvider resourceTokenProvider() {
    return resourceTokenProvider;
  }

  /**
   * The {@link JsonSupport} used by the client.
   */
  public JsonSupport jsonSupport() {
    return jsonSupport;
  }

  /**
   * The {@link MetricCollector} used by the client.
   */
  public MetricCollector metricCollector() {
    return metricCollector;
  }

  /**
   * Access API resources from the client.
   */
  public Resources resources() {
    return resources;
  }

  @SuppressWarnings("WeakerAccess")
  public static class Builder {

    private URI baseURI;
    private JsonSupport jsonSupport;
    private ResourceProvider resourceProvider;
    private ResourceTokenProvider resourceTokenProvider;
    private MetricCollector metricCollector;
    private long connectTimeout;
    private long readTimeout;
    private boolean enableHttpLogging;

    Builder() {
      connectTimeout = 20_000;
      readTimeout = 20_000;
    }

    /**
     * Construct the client. The {@link ResourceProvider}, {@link ResourceTokenProvider},
     * {@link JsonSupport}, connection and read timeouts will use defaults if not
     * supplied. The base URI must be provided.
     *
     * @return a new client.
     */
    public NakadiClient build() {

      NakadiException.throwNonNull(baseURI, "Please provide a base URI");

      if (jsonSupport == null) {
        jsonSupport = new GsonSupport();
      }

      if (resourceTokenProvider == null) {
        resourceTokenProvider = new EmptyResourceTokenProvider();
      }

      logger.info("Loaded resource token provider {}", resourceTokenProvider.getClass().getName());

      if (metricCollector == null) {
        metricCollector = new MetricCollectorDevnull();
      }

      logger.info("Loaded metric collector {}", metricCollector.getClass().getName());

      metricCollector = new MetricCollectorSafely(metricCollector);

      if (resourceProvider == null) {

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
            .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
            .readTimeout(readTimeout, TimeUnit.MILLISECONDS);

        if (enableHttpLogging) {
          builder = builder.addNetworkInterceptor(
              new HttpLoggingInterceptor().setLevel(HttpLoggingInterceptor.Level.BODY));
          logger.info("Enabled http tracing");
        }

        resourceProvider =
            new OkHttpResourceProvider(baseURI, builder.build(), jsonSupport, metricCollector);
      }

      return new NakadiClient(this);
    }

    /**
     * Turn on http request/response logging. The http traffic will be logged at info.
     *
     * @return this builder
     */
    public Builder enableHttpLogging() {
      enableHttpLogging = true;
      return this;
    }

    /**
     * Optionally set the default connect timeout for new connections. If 0, no timeout, otherwise
     * values must be between 1 and {@link Integer#MAX_VALUE}. The default is 20s.
     * <p>
     * For consuming streams, the read timeout may be independently set via a
     * {@link StreamConfiguration}, otherwise streams default to this setting.
     * </p>
     */
    public Builder connectTimeout(long timeout, TimeUnit unit) {
      connectTimeout = unit.toMillis(timeout);
      return this;
    }

    /**
     * Optionally set the default read timeout for connections. If 0, no timeout, otherwise
     * values must be between 1 and {@link Integer#MAX_VALUE}. The default is 20s.
     * <p>
     * For consuming streams, the read timeout may be independently set via a
     * {@link StreamConfiguration}, otherwise streams default to this setting.
     * </p>
     */
    public Builder readTimeout(long timeout, TimeUnit unit) {
      readTimeout = unit.toMillis(timeout);
      return this;
    }

    /**
     * Optionally set the token provider. Not setting a provider will mean no bearer tokens are
     * sent to the server by default.
     */
    public Builder resourceTokenProvider(ResourceTokenProvider resourceTokenProvider) {
      this.resourceTokenProvider = resourceTokenProvider;
      return this;
    }

    /**
     * Optionally set the {@link MetricCollector}. Not setting the collector will mean no metrics
     * are recorded.
     */
    public Builder metricCollector(MetricCollector metricCollector) {
      this.metricCollector = metricCollector;
      return this;
    }

    /**
     * Set the required base URI for the server. All other resource URLs are derived from this.
     */
    public Builder baseURI(URI baseURI) {
      this.baseURI = baseURI;
      return this;
    }

    /**
     * Set the required base URI for the server. All other resource URLs are derived from this.
     */
    public Builder baseURI(String baseURI) {
      try {
        baseURI(new URI(baseURI));
        return this;
      } catch (URISyntaxException e) {
        throw new NakadiException(
            Problem.localProblem(String.format("Bad base URI [%s]", baseURI), e.getMessage()), e);
      }
    }
  }
}
