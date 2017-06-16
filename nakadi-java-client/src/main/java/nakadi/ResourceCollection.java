package nakadi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Carries a page of results from the server. Concrete subclass will define T as the member type of
 * the collection, and implement the {@link #nextPage()} method.
 *
 * To read just this page call {@link #items()}. To see if there's more data in the collection call
 * {@link #hasNextLink()}}.
 *
 * To set up automatic iterating over the collection, call {@link #iterable()}}. This will perform
 * pagination in the background and present items via an {@link Iterable}. Concrete classes are
 * asked to implement {@link #emptyPage()} or {@link #fetchPage}.
 *
 * @param <T> the type of the resource as defined by subclasses.
 */
abstract public class ResourceCollection<T> {

  private static final String REL_NEXT = "next";

  private final List<T> items;
  private final List<ResourceLink> links;
  protected final NakadiClient client;
  protected boolean hasNextLink;
  private volatile ResourceCollectionEmpty<T> empty;

  public ResourceCollection(List<T> items, List<ResourceLink> links, NakadiClient client) {
    this.items = items;
    this.links = links;
    this.client = client;
    this.hasNextLink = determineHasNextLink(this.links);
  }

  /**
   * Fetch the next page delegating to the concrete collection. This allows the concrete collection
   * to control response marshalling and details like authorization or media type controls.
   *
   * If there is no next page, this <b>must</b> return an empty collection - failing to do so will
   * result in ResourceCollection throwing a {@link NullPointerException} at the call site.
   *
   * @return the next page in the collection, empty if there's no next page.
   */
  abstract public ResourceCollection<T> fetchPage(String url);

  /**
   * Return an empty page.
   *
   * @return an empty collection
   */
  public ResourceCollection<T> emptyPage() {
    if (empty == null) {
      empty = new ResourceCollectionEmpty<>(); // can't create this in the ctor
    }
    return empty;
  }

  /**
   * Ask for the next page.
   *
   * This will throw a {@link NullPointerException} if either the {@link #emptyPage()} or
   * {@link #fetchPage} abstract methods return nulls.
   *
   * @return the next page, or an empty page if there is no next link
   */
  public ResourceCollection<T> nextPage() {
    if (this.hasNextLink()) {
      Optional<URI> link = this.findLink(ResourceCollection.REL_NEXT);
      if (link.isPresent()) {
        String url = resolveNextPageLink(link);
        final ResourceCollection<T> collection = fetchPage(url);
        NakadiException.throwNonNull(collection,
            String.format("Concrete collection %s must provide a non-null page", this.getClass()));

        hasNextLink = collection.hasNextLink();
        return collection;
      }
    }

    ResourceCollection<T> empty = emptyPage();
    NakadiException.throwNonNull(empty,
        String.format("Concrete collection %s must provide a non-null empty page",
            this.getClass()));
    return empty;
  }

  private String resolveNextPageLink(Optional<URI> link) {
    final URI uri = link.get();

    String url;

    final String host = uri.getHost();
    if(host == null) {
      final URI baseURI = client.baseURI();

      final URI pageUri;
      try {
        pageUri = new URI(
            baseURI.getScheme(),
            baseURI.getUserInfo(),
            baseURI.getHost(),
            baseURI.getPort(),
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment()
        );
      } catch (URISyntaxException e) {
        throw new NakadiException(Problem.localProblem("pagination_err", e.getMessage()), e);
      }
      url = pageUri.toASCIIString();
    } else {
      url = uri.toASCIIString();
    }
    return url;
  }

  /**
   * The items in this page of the collection.
   *
   * @return the items in this page
   */
  public List<T> items() {
    return items;
  }

  /**
   * The list of rel links returned with the page. Typically we're looking for the 'next' link to
   * paginate with.
   *
   * @return any links the collection responded with.
   */
  public List<ResourceLink> links() {
    return links;
  }

  /**
   * Ask if the collection responded with a rel link of 'next'.
   *
   * @return true if there's a rel link with the value 'next'.
   */
  public boolean hasNextLink() {
    return hasNextLink;
  }

  /**
   * Lookup the returned links for a particular rel value.
   *
   * @param rel the link relation
   * @return the URI for the rel, or empty
   */
  public Optional<URI> findLink(String rel) {
    NakadiException.throwNonNull(rel, "Please provide a rel value");
    URI uri = null;
    for (ResourceLink link : links) {
      if (rel.equals(link.rel())) {
        uri = link.href();
        break;
      }
    }
    return Optional.ofNullable(uri);
  }

  /**
   * Get an iterable for this page of the collection. Handy if you want to use foreach.
   *
   * @return an Iterable for this page.
   */
  public Iterable<T> iterable() {
    return new ResourceCollectionIterable<>(this);
  }

  @SuppressWarnings({"WeakerAccess", "VisibleForTesting"})
  boolean determineHasNextLink(List<ResourceLink> links) {
    boolean next = false;
    for (ResourceLink link : links) {
      if (REL_NEXT.equals(link.rel())) {
        next = true;
        break;
      }
    }
    return next;
  }

  @Override public int hashCode() {
    return Objects.hash(items, links, hasNextLink, empty);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ResourceCollection)) return false;
    ResourceCollection<?> that = (ResourceCollection<?>) o;
    return hasNextLink == that.hasNextLink &&
        Objects.equals(items, that.items) &&
        Objects.equals(links, that.links) &&
        Objects.equals(empty, that.empty);
  }

  @Override public String toString() {
    return this.getClass().getSimpleName() + "{" +
        "items=" + items +
        ", links=" + links +
        ", hasNextLink=" + hasNextLink +
        ", empty=" + empty +
        '}';
  }
}
