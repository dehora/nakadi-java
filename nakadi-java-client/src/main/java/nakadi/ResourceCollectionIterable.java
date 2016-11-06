package nakadi;

import java.util.Iterator;

/**
 * @param <T> the type of the resource as defined by subclasses.
 */
class ResourceCollectionIterable<T> implements Iterable<T> {

  private final ResourceCollection<T> collection;

  ResourceCollectionIterable(ResourceCollection<T> collection) {
    this.collection = collection;
  }

  @Override public Iterator<T> iterator() {
    return new ResourceCollectionIterator<>(collection);
  }
}
