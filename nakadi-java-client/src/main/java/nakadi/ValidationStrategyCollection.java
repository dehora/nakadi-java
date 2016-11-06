package nakadi;

import java.util.List;

public class ValidationStrategyCollection extends ResourceCollection<String> {

  private final RegistryResource registryResource;

  public ValidationStrategyCollection(List<String> items, List<ResourceLink> links,
      RegistryResource registryResourceesource) {
    super(items, links);
    this.registryResource = registryResourceesource;
  }

  public ResourceCollection<String> fetchPage(String url) {
    return registryResource.loadValidationPage(url);
  }
}
