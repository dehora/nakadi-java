package nakadi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import okhttp3.logging.HttpLoggingInterceptor;

/**
 * A client which can be used to work with the server. Clients must be created via a builder.
 * Once built, callers can access the server via the resources available from the
 * {@link #resources()} method.
 */
public class NakadiClient {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final String VERSION = Version.VERSION;
  static final String USER_AGENT = "nakadi-java/" + NakadiClient.VERSION;
  // capture some client side jvm data for diagnostics
  private static List<String> jvmData = new ArrayList<>();
  static final String PLATFORM_DETAILS_JSON = GsonSupport.gsonCompressed()
      .toJson(jvmData.stream().collect(Collectors.toMap(String::toString, System::getProperty)));

  static {
    jvmData.add("os.arch");
    jvmData.add("os.name");
    jvmData.add("os.version");
    jvmData.add("java.class.version");
    jvmData.add("java.runtime.version");
    jvmData.add("java.vm.name");
    jvmData.add("java.vm.version");
  }

  private final URI baseURI;
  private final JsonSupport jsonSupport;
  private final ResourceProvider resourceProvider;
  private final TokenProvider tokenProvider;
  private final MetricCollector metricCollector;
  private final Resources resources;
  private final String certificatePath;

  private NakadiClient(Builder builder) {
    NakadiException.throwNonNull(builder.baseURI, "Please provide a base URI.");
    this.baseURI = builder.baseURI;
    this.jsonSupport = builder.jsonSupport;
    this.resourceProvider = builder.resourceProvider;
    this.tokenProvider = builder.tokenProvider;
    this.metricCollector = builder.metricCollector;
    this.resources = new Resources(this);
    this.certificatePath = builder.certificatePath;
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
   * The {@link TokenProvider} used by the client.
   */
  public TokenProvider resourceTokenProvider() {
    return tokenProvider;
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

  public String certificatePath() {
    return certificatePath;
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
    private TokenProvider tokenProvider;
    private MetricCollector metricCollector;
    private long connectTimeout;
    private long readTimeout;
    private boolean enableHttpLogging;
    private String certificatePath;

    Builder() {
      connectTimeout = 20_000;
      readTimeout = 20_000;
    }

    /**
     * Construct the client. The {@link ResourceProvider}, {@link TokenProvider},
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

      if (tokenProvider == null) {
        tokenProvider = new EmptyTokenProvider();
      }

      logger.info("Loaded resource token provider {}", tokenProvider.getClass().getName());

      if (metricCollector == null) {
        metricCollector = new MetricCollectorDevnull();
      }

      logger.info("Loaded metric collector {}", metricCollector.getClass().getName());

      metricCollector = new MetricCollectorSafely(metricCollector);

      if (resourceProvider == null) {
        resourceProvider = buildResourceProvider();
      }

      return new NakadiClient(this);
    }

    private ResourceProvider buildResourceProvider() {
      OkHttpClient.Builder builder = new OkHttpClient.Builder()
          .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
          .readTimeout(readTimeout, TimeUnit.MILLISECONDS);

      if (certificatePath != null) {
        new SecuritySupport(certificatePath).applySslSocketFactory(builder);
      }

      if (enableHttpLogging) {
        builder = builder.addNetworkInterceptor(
            new HttpLoggingInterceptor(new okhttp3.logging.HttpLoggingInterceptor.Logger() {

              final Logger httpLogger = LoggerFactory.getLogger("NakadiClientHttpLog");

              @Override public void log(String message) {
                httpLogger.info(message);
              }
            })
                .setLevel(HttpLoggingInterceptor.Level.BODY));
        logger.info("Enabled http tracing");
      }

      return new OkHttpResourceProvider(builder.build(), jsonSupport, metricCollector);
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
    public Builder tokenProvider(TokenProvider tokenProvider) {
      this.tokenProvider = tokenProvider;
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

    public Builder certificatePath(String path) {
      this.certificatePath = path;
      return this;
    }
  }
}
