package nakadi;

import java.util.Optional;

/**
 * Dummy to return no tokens to the client. Suitable for development.
 */
public class EmptyTokenProvider implements TokenProvider {

  @Override public Optional<String> authHeaderValue() {
    return Optional.empty();
  }
}
