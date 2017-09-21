package nakadi;

import java.util.Optional;

/**
 * Supplied to {@link NakadiClient} and provides a token string that can be used
 * to authorize requests against the server. Acts as a {@link FunctionalInterface} and
 * can be supplied as a lambda expression.
 */
@FunctionalInterface
public interface TokenProvider {

  /**
   * <p>
   * Scopes have been removed in recent Nakadi versions. Scopes set here are ignored.
   * </p>
   *
   * @return a value suitable for use in an Authorization header, or null to suppress the
   * Authorization header being set
   */
  Optional<String> authHeaderValue(@Deprecated String scope);

}
