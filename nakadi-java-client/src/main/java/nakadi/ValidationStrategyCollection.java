package nakadi;

import java.util.List;

/**
 * Represents the server validation strategies.
 */
public class ValidationStrategyCollection extends ResourceCollection<String> {

  private final RegistryResource registryResource;

  public ValidationStrategyCollection(List<String> items, List<ResourceLink> links,
      RegistryResource registryResource, NakadiClient client) {
    super(items, links, client);
    this.registryResource = registryResource;
  }

  public ResourceCollection<String> fetchPage(String url) {
    return registryResource.loadValidationPage(url);
  }
}
