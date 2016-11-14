package nakadi;

import java.util.Map;

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

  Response create(Subscription subscription)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  Subscription find(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  SubscriptionCollection list()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  SubscriptionCollection list(QueryParams params)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  Response delete(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  CursorCommitResultCollection checkpoint(Map<String, String> context, Cursor... cursors)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, ContractException, NakadiException;

  SubscriptionCursorCollection cursors(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  SubscriptionEventTypeStatsCollection stats(String id)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;
}
