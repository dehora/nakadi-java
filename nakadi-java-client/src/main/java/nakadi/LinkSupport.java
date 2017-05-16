package nakadi;

import java.util.ArrayList;
import java.util.List;

class LinkSupport {

  List<ResourceLink> toLinks(PaginationLinks _links) {
    List<ResourceLink> links = new ArrayList<>();
    if (_links != null) {
      final PaginationLink prev = _links.prev();
      final PaginationLink next = _links.next();

      if (prev != null) {
        links.add(new ResourceLink("prev", prev.href()));
      }

      if (next != null) {
        links.add(new ResourceLink("next", next.href()));
      }
    }
    return links;
  }
}
