package nakadi;

import java.util.Collection;

public interface EventResource {

  <T> Response send(String eventTypeName, Collection<T> events);

  <T> Response send(String eventTypeName, T event);

}
