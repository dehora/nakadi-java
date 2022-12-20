package nakadi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client which can be used to work with the server. Clients must be created via a builder.
 * Once built, callers can access the server via the resources available from the
 * {@link #resources()} method.
 */
public class NakadiClient {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final String VERSION = Version.VERSION;
  static final String USER_AGENT = "nakadi-java/" + NakadiClient.VERSION;

  private final URI baseURI;
  private final JsonSupport jsonSupport;
  private final ResourceProvider resourceProvider;
  private final TokenProvider tokenProvider;
  private final MetricCollector metricCollector;
  private final Resources resources;
  private final String certificatePath;
  private final boolean enablePublishingCompression;
  private final CompressionSupport compressionSupport;
  private final SerializationSupport serializationSupport;

  private NakadiClient(Builder builder) {
    NakadiException.throwNonNull(builder.baseURI, "Please provide a base URI.");
    this.baseURI = builder.baseURI;
    this.jsonSupport = builder.jsonSupport;
    this.resourceProvider = builder.resourceProvider;
    this.tokenProvider = builder.tokenProvider;
    this.metricCollector = builder.metricCollector;
    this.resources = new Resources(this);
    this.certificatePath = builder.certificatePath;
    this.enablePublishingCompression = builder.enablePublishingCompression;
    this.compressionSupport = builder.compressionSupport;
    this.serializationSupport = builder.serializationSupport;
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
   * The {@link CompressionSupport} used by the client.
   */
  public CompressionSupport compressionSupport() {
    return compressionSupport;
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

  // only needs to be seen by package code
  boolean enablePublishingCompression() {
    return enablePublishingCompression;
  }

  /**
   * Access API resources from the client.
   */
  public Resources resources() {
    return resources;
  }

  public SerializationSupport getSerializationSupport() {
    return serializationSupport;
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
    private long writeTimeout;
    private boolean enableHttpLogging;
    private boolean enablePublishingCompression;
    private CompressionSupport compressionSupport;
    private String certificatePath;
    private SerializationSupport serializationSupport;

    Builder() {
      connectTimeout = 20_000;
      readTimeout = 20_000;
      writeTimeout = 10_000;
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

      if (serializationSupport == null) {
        serializationSupport = JsonSerializationSupport.newInstance(jsonSupport);
      }

      if (compressionSupport == null) {
        compressionSupport = new CompressionSupportGzip();
      }

      logger.info("Loaded compression support {}", compressionSupport.getClass().getName());

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
          .readTimeout(readTimeout, TimeUnit.MILLISECONDS)
          .writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);

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
     * Turn on compression for event posting.
     *
     * @return this builder
     */
    public Builder enablePublishingCompression() {
      enablePublishingCompression = true;
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
     * Optionally set the default write timeout for connections. If 0, no timeout, otherwise
     * values must be between 1 and {@link Integer#MAX_VALUE}. The default is 10s.
     * <p>
     * The write timeout may be independently per request set via a
     * {@link Resource#writeTimeout(long, TimeUnit)}, otherwise requests default to this setting.
     * </p>
     */
    public Builder writeTimeout(long timeout, TimeUnit unit) {
      writeTimeout = unit.toMillis(timeout);
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

    /**
     * Optionally set the {@link JsonSupport}.
     *
     * @return this
     */
    public Builder jsonSupport(JsonSupport jsonSupport) {
      this.jsonSupport = jsonSupport;
      return this;
    }

    public Builder serializationSupport(SerializationSupport serializationSupport) {
      this.serializationSupport = serializationSupport;
      return this;
    }
  }
}
