package nakadi;

import java.net.URI;

/**
 * A server supplied link and its relation type.
 */
public class ResourceLink {

  private final String rel;
  private final URI href;

  public ResourceLink(String rel, URI href) {
    this.rel = rel;
    this.href = href;
  }

  /**
   * @return the link relation
   */
  public String rel() {
    return rel;
  }

  /**
   * @return the link URI
   */
  public URI href() {
    return href;
  }
}
