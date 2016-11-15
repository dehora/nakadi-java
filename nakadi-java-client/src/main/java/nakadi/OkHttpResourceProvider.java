package nakadi;

import okhttp3.OkHttpClient;

class OkHttpResourceProvider implements ResourceProvider {

  private final OkHttpClient okHttpClient;
  private final JsonSupport jsonSupport;
  private MetricCollector metricCollector;

  public OkHttpResourceProvider(OkHttpClient okHttpClient, JsonSupport jsonSupport,
      MetricCollector metricCollector) {

    this.okHttpClient = okHttpClient;
    this.jsonSupport = jsonSupport;
    this.metricCollector = metricCollector;
  }

  @Override public Resource newResource() {
    return new OkHttpResource(okHttpClient, jsonSupport, metricCollector);
  }
}
