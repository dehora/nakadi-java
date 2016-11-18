package nakadi;

import java.util.Map;
import java.util.Optional;

/**
 * Supports API operations related to subscriptions.
 */
public interface SubscriptionResource {

  /**
   * Set the OAuth scope to be used for the request. This can be reset between requests.
   *
   * @param scope the OAuth scope to be used for the request
   * @return this
   */
  SubscriptionResource scope(String scope);

  /**
   * Set the retry policy to be used for the request. This can be reset between requests. Setting
   * it to null (the default) disables retries.
   *
   * @param retryPolicy the retry policy
   * @return this
   */
  SubscriptionResource retryPolicy(RetryPolicy retryPolicy);

  /**
   * Create a new subscription on the server.
   *
   * @param subscription the new subscription
   * @return the response result
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NakadiException for a general exception
   */
  Response createReturningResponse(Subscription subscription)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, ConflictException, NakadiException;

  /**
   * Create a new subscription on the server.
   *
   * @param subscription the new subscription
   * @return the subscription result
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws ConflictException for a 409
   * @throws NakadiException for a general exception
   */
  Subscription create(Subscription subscription)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, ConflictException, NakadiException;

  /**
   * Find a subscription by id. Throw NotFoundException for a 404.
   *
   * @param id the subscription id
   * @return the subscription result
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  Subscription find(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NotFoundException, NakadiException;

  /**
   * Try and find a subscription by id.
   *
   * @param id the subscription id
   * @return the subscription result or {@link Optional#empty()}
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NakadiException for a general exception
   */
  Optional<Subscription> tryFind(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * List the subscriptions on the server.
   *
   * @return a list that will automatically page when iterated.
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  SubscriptionCollection list()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NotFoundException, NakadiException;

  /**
   * List the subscriptions on the server according to query optipns.
   *
   * @return a list that will automatically page when iterated.
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  SubscriptionCollection list(QueryParams params)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Delete a subscription by id.
   *
   * @param id the subscription id
   * @return the response
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NotFoundException for a 404
   * @throws NakadiException for a general exception
   */
  Response delete(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NotFoundException, NakadiException;

  /**
   * Commit a checkpoint.
   *
   * @param context contains the stream id header and subscription id.
   * @param cursors the cursors to commit
   * @return the result
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NotFoundException for a 404
   * @throws ContractException for a "successful" request but which does not follow the api
   * @throws NakadiException  for a general exception
   */
  CursorCommitResultCollection checkpoint(Map<String, String> context, Cursor... cursors)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NotFoundException, NakadiException;

  /**
   * Find the cursors for a subscription.
   *
   * @param id the subscription id
   * @return the response as a collection of cursors
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NotFoundException for a 404
   * @throws NakadiException  for a general exception
   */
  SubscriptionCursorCollection cursors(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NotFoundException, NakadiException;

  /**
   * The stats for a subscription.
   *
   * @param id the subscription id
   * @return the response as a collection of cursors
   * @throws AuthorizationException unauthorised
   * @throws ClientException for a 400 or generic client error
   * @throws ServerException for a 400 or generic client error
   * @throws InvalidException for a 422
   * @throws RateLimitException for a 429
   * @throws NotFoundException for a 404
   * @throws NakadiException  for a general exception
   */
  SubscriptionEventTypeStatsCollection stats(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NotFoundException, NakadiException;
}
