package nakadi;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provide an {@link Iterator} for a {@link ResourceCollection}. The iterator checks the local
 * page for items and if that's exhausted, checks if the page has a rel of next. If it does the
 * rel's link will be resolved to get the next page of items.
 *
 * @param <T> the type of the resource as defined by subclasses.
 */
class ResourceCollectionIterator<T> implements Iterator<T> {

  private Iterator<T> collectionItemsIterator;
  private ResourceCollection<T> collection;

  ResourceCollectionIterator(ResourceCollection<T> collection) {
    this.collection = collection;
    this.collectionItemsIterator = collection.items().iterator();
  }

  @Override public boolean hasNext() {
    return collectionItemsIterator.hasNext() || collection.hasNextLink();
  }

  @Override public T next() {
    if (!collectionItemsIterator.hasNext()) {
      loadNextPage();
    }

    if (collectionItemsIterator.hasNext()) {
      return collectionItemsIterator.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  private void loadNextPage() {
    if (collection.hasNextLink()) {
      collection = collection.nextPage();
      collectionItemsIterator = collection.items().iterator();
    }
  }
}
