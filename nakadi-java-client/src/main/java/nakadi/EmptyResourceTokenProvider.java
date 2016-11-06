package nakadi;

import java.util.Optional;

public class EmptyResourceTokenProvider implements ResourceTokenProvider {

  @Override public Optional<ResourceToken> supplyToken() {
    return Optional.empty();
  }
}
