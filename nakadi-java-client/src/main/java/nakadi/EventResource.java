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

  /**
   * Set the retry policy to be used for the request. This can be reset between requests. Setting
   * it to null (the default) disables retries.
   * <p>
   *   <b>Warning: </b> the ordering and general delivery behaviour for event delivery is
   *   undefined under retries. That is, a delivery retry may result in out or order events being
   *   sent to the server.
   * </p>
   *
   * @param retryPolicy the retry policy
   * @return this
   */
  EventResource retryPolicy(RetryPolicy retryPolicy);

  /**
   * Send a batch of events to the server.
   *
   * <p>
   *   The response may be a 207 indicating some or none of the events succeeded. See the Nakadi
   *   API definition for details.
   * </p>
   * <p>
   *   <b>Warning: </b> the ordering and general delivery behaviour for event delivery is
   *   undefined under retries. That is, a delivery retry may result in out or order batches being
   *   sent to the server. Also retrying a partially delivered (207) batch may result in one
   *   or more events being delivered multiple times.
   * </p>
   *
   * @param eventTypeName the event type name
   * @param events the events
   * @param <T> the type of the events
   * @return the response
   */
  <T> Response send(String eventTypeName, Collection<T> events);

  /**
   * Send an event to the server.
   *
   * <p>
   *   <b>Warning: </b> the ordering and general delivery behaviour for event delivery is
   *   undefined under retries. That is, a delivery retry may result in out or order events being
   *   sent to the server.
   * </p>
   *
   * @param eventTypeName the event type name
   * @param event the event
   * @param <T> the type of the event
   * @return the response
   */
  <T> Response send(String eventTypeName, T event);

}
