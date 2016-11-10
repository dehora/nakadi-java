package nakadi;

import java.util.List;

/**
 * Represents the enrichments supported by the server.
 */
public class EnrichmentStrategyCollection extends ResourceCollection<String> {

  private final RegistryResource registryResource;

  public EnrichmentStrategyCollection(List<String> items, List<ResourceLink> links,
      RegistryResource registryResource) {
    super(items, links);
    this.registryResource = registryResource;
  }

  public ResourceCollection<String> fetchPage(String url) {
    return registryResource.loadEnrichmentPage(url);
  }
}

