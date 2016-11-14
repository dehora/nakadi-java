package nakadi;

import java.util.Collection;

public interface EventResource {

  /**
   * Set the OAuth scope to be used for the request. This can be reset between requests.
   *
   * @param scope the OAuth scope to be used for the request
   * @return this
   */
  EventResource scope(String scope);

  <T> Response send(String eventTypeName, Collection<T> events);

  <T> Response send(String eventTypeName, T event);

}
