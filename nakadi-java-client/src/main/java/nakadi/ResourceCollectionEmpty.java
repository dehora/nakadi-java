package nakadi;

import java.util.ArrayList;

// todo: see how much alloc overhead this causes
class ResourceCollectionEmpty<T> extends ResourceCollection<T> {

  volatile ResourceCollectionEmpty<T> dummy;

  public ResourceCollectionEmpty() {
    super(new ArrayList<>(), new ArrayList<>());
  }

  @Override public ResourceCollection<T> fetchPage(String url) {
    return emptyPage();
  }

  @Override public ResourceCollection<T> emptyPage() {
    if (dummy == null) {
      dummy = new ResourceCollectionEmpty<>();
    }
    return dummy;
  }
}
