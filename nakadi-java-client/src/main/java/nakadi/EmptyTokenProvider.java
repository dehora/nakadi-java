package nakadi;

/**
 * Dummy to return no tokens to the client. Suitable for development.
 */
public class EmptyTokenProvider implements TokenProvider {

  @Override public String authHeaderValue() {
    return null;
  }
}
