package nakadi;

import java.util.List;
import java.util.Optional;

/**
 * Supports API operations related to event types.
 */
public interface EventTypeResource {

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0. Scopes set here are ignored.
   *
   * @param scope the OAuth scope to be used for the request
   * @return this
   */
  @Deprecated
  EventTypeResource scope(String scope);

  /**
   * Set the retry policy to be used for the request. This can be reset between requests. Setting
   * it to null (the default) disables retries.
   *
   * @param retryPolicy the retry policy
   * @return this
   */
  EventTypeResource retryPolicy(RetryPolicy retryPolicy);

  /**
   * Create an event type.
   *
   * @param eventType an event type
   * @return a http response
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NakadiException for a general exception
   */
  Response create(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Update an existing event type
   *
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  Response update(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Find an event type. Throws NotFoundException if not found.
   *
   * @param eventTypeName the event type name
   * @return the event type
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  EventType findByName(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException,
      RateLimitException, NotFoundException, NakadiException;

  /**
   * Try and find an event type.
   *
   * @param eventTypeName the event type name or {@link Optional#empty()} if not found
   * @return the event type
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NakadiException for a general exception
   */
  Optional<EventType> tryFindByName(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException,
      RateLimitException, NotFoundException, NakadiException;

  /**
   * Successful deletes return 200 and no body.
   *
   * @return a http response that will have no body
   */
  Response delete(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * @return a collection of event types
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  EventTypeCollection list()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Fetch the partitions for an event type.
   *
   * @param eventTypeName the event type
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  PartitionCollection partitions(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Fetch the partition for an event type.
   *
   * @param eventTypeName the event type
   * @param partitionId the partition
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  Partition partition(String eventTypeName, String partitionId)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Fetch the partition for an event type.
   *
   * @param eventTypeName the event type
   * @param partitionId the partition
   * @param params the query parameters
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  Partition partition(String eventTypeName, String partitionId, QueryParams params)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * @param eventTypeName the event type
   * @return the event type schemas for the event type.
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 5xx or generic server error
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  @Experimental
  EventTypeSchemaCollection schemas(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException,
      RateLimitException, NakadiException;

  /**
   * Make a request to shift cursors.
   *
   * @param eventTypeName the event type
   * @param cursorList the cursors to shift
   * @return the resulting shifted cursors
   */
  CursorCollection shift(String eventTypeName, List<Cursor> cursorList);

  /**
   * Request a calculation of the distance between one or more pairs of cursor offsets.
   *
   * @param eventTypeName the event type
   * @param cursorDistanceList the cursors distances to check.
   * @return the result from the server.
   */
  CursorDistanceCollection distance(String eventTypeName, List<CursorDistance> cursorDistanceList);

  /**
   * Check the the number of unconsumed events for each cursor's partition.
   *
   * @param eventTypeName the event type
   * @param cursors the cursors to check.
   * @return the result from the server.
   */
  PartitionCollection lag(String eventTypeName, List<Cursor> cursors);
}
