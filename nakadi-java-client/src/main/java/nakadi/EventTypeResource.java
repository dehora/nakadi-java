package nakadi;

/**
 * Supports API operations related to event types.
 */
public interface EventTypeResource {

  /**
   * Set the OAuth scope to be used for the request. This can be reset between requests.
   *
   * @param scope the OAuth scope to be used for the request
   * @return this
   */
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
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  Response create(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Update an existing event type
   *
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  Response update(EventType eventType)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * @param eventTypeName the event type name
   */
  EventType findByName(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

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
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  EventTypeCollection list()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Fetch the partitions for an event type.
   *
   * @param eventTypeName the event type
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  PartitionCollection partitions(String eventTypeName)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Fetch the partition for an event type.
   *
   * @param eventTypeName the event type
   * @param partitionId the partition
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  Partition partition(String eventTypeName, String partitionId)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;
}
