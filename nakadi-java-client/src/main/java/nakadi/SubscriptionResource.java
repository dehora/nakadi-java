package nakadi;

import java.util.Map;

/**
 * Supports API operations related to subscriptions.
 */
public interface SubscriptionResource {

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
