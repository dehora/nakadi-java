package nakadi;

import java.util.Collection;
import java.util.List;

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
   *<p>
   *   If the first item in events is detected to be a String, the list will be treated as raw
   *   JSON items. Otherwise the event is serialised to JSON. This behaviour is fragile and
   *   might be changed prior to 1.0.0, most likely by introducing a new signature for raw
   *   JSON events.
   * </p>
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
   * <p>
   *   If the event is detected to be a String, it will be treated as raw JSON. Otherwise the
   *   event is serialised to JSON.
   * </p>
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


  /**
   * Send a batch of events to the server.
   *
   * <p>
   *   If the response is 422 or 207 the BatchItemResponseCollection will contain items.
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
   * @return a BatchItemResponseCollection which will be empty if successful or have items
   * if the post was partially successful (via a 422 or 207 response)
   */
  <T> BatchItemResponseCollection sendBatch(String eventTypeName, List<T> events);

}
