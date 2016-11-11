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
   * @return a value suitable for use in an Authorization header, or null to suppress the
   * Authorization header being set
   */
  String authHeaderValue();

}
