package nakadi;

import java.net.URI;
import okhttp3.OkHttpClient;

class OkHttpResourceProvider implements ResourceProvider {

  private final URI baseURI;
  private final OkHttpClient okHttpClient;
  private final JsonSupport jsonSupport;
  private MetricCollector metricCollector;
  private NakadiClient client;

  public OkHttpResourceProvider(URI baseURI, OkHttpClient okHttpClient, JsonSupport jsonSupport,
      MetricCollector metricCollector) {

    this.baseURI = baseURI;
    this.okHttpClient = okHttpClient;
    this.jsonSupport = jsonSupport;
    this.metricCollector = metricCollector;
  }

  @Override public Resource newResource() {
    return new OkHttpResource(okHttpClient, jsonSupport, metricCollector);
  }
}
