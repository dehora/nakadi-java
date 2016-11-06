package nakadi;

import com.google.common.collect.Lists;

// todo: see how much alloc overhead this causes
class ResourceCollectionEmpty<T> extends ResourceCollection<T> {

  volatile ResourceCollectionEmpty<T> dummy;

  public ResourceCollectionEmpty() {
    super(Lists.newArrayList(), Lists.newArrayList());
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
