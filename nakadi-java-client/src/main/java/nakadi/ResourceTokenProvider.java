package nakadi;

import java.util.Optional;

/**
 * Supplied to {@link NakadiClient} and provides {@link ResourceToken} objects that can be used
 * to authorize requests against the server. Acts as a {@link FunctionalInterface} and
 * can be supplied as a lambda expression.
 */
@FunctionalInterface
public interface ResourceTokenProvider {

  /**
   * Return an optional {@link ResourceToken}. If the {@link Optional} is empty then
   * the client will not set an Authorization header on the request - this is typically useful
   * for local development.
   *
   * @return maybe a {@link ResourceToken}
   */
  Optional<ResourceToken> supplyToken();
}
