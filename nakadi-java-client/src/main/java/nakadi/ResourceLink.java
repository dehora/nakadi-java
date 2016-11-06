package nakadi;

import java.net.URI;

public class ResourceLink {

  private final String rel;
  private final URI href;

  public ResourceLink(String rel, URI href) {
    this.rel = rel;
    this.href = href;
  }

  public String rel() {
    return rel;
  }

  public URI href() {
    return href;
  }
}
