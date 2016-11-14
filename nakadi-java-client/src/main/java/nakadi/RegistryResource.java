package nakadi;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Supports API operations related to the registry.
 */
public class RegistryResource {

  private static final String PATH = "registry";
  private static final String PATH_VALIDATION_STRATEGIES = "validation-strategies";
  private static final String PATH_ENRICHMENT_STRATEGIES = "enrichment-strategies";
  private static final String APPLICATION_JSON = "application/json";
  private static final Type TYPE = new TypeToken<List<String>>() {
  }.getType();

  private final NakadiClient client;

  public RegistryResource(NakadiClient client) {
    this.client = client;
  }

  /**
   * @return the validation strategies on the server.
   */
  public ValidationStrategyCollection listValidationStrategies() {
    return loadValidationPage(collection(PATH, PATH_VALIDATION_STRATEGIES).buildString());
  }

  /**
   * @return the enrichment strategies on the server.
   */
  public EnrichmentStrategyCollection listEnrichmentStrategies() {
    return loadEnrichmentPage(collection(PATH, PATH_ENRICHMENT_STRATEGIES).buildString());
  }

  ValidationStrategyCollection loadValidationPage(String url) {
    return new ValidationStrategyCollection(loadCollection(url), Lists.newArrayList(), this);
  }

  EnrichmentStrategyCollection loadEnrichmentPage(String url) {
    return new EnrichmentStrategyCollection(loadCollection(url), Lists.newArrayList(), this);
  }

  private List<String> loadCollection(String url) {
    ResourceOptions options = ResourceSupport.options(APPLICATION_JSON)
        .tokenProvider(client.resourceTokenProvider());
    Response response = client.resourceProvider()
        .newResource()
        .retryPolicy(policyBackoffForCollectionPage())
        .requestThrowing(Resource.GET, url, options);

    return client.jsonSupport().fromJson(response.responseBody().asString(), TYPE);
  }

  private RetryPolicy policyBackoffForCollectionPage() {
    return ExponentialRetry.newBuilder()
        .initialInterval(1000, TimeUnit.MILLISECONDS)
        .maxAttempts(5)
        .maxInterval(6000, TimeUnit.MILLISECONDS)
        .build();
  }

  private UriBuilder collection(String basePath, String path) {
    return UriBuilder.builder(client.baseURI()).path(basePath).path(path);
  }
}
