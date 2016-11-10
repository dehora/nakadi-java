package nakadi;

import java.util.Optional;

/**
 * Dummy to return no tokens to the client. Suitable for development.
 */
public class EmptyResourceTokenProvider implements ResourceTokenProvider {

  /**
   * @return {@link Optional#empty}
   */
  @Override public Optional<ResourceToken> supplyToken() {
    return Optional.empty();
  }
}
