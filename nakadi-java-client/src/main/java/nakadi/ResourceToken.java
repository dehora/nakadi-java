package nakadi;

/**
 * Represents an access token for the server. Acts as a {@link FunctionalInterface} and
 * can be supplied as a lambda expression.
 * <p>
 * Underlying implementations will be typically be generating an OAuth Bearer token.
 */
@FunctionalInterface
public interface ResourceToken {

  /**
   * The value to set in an <code>Authorization</code> header.
   *
   * @return a value that can be set in an <code>Authorization</code>  header.
   */
  String asAuthorizationHeaderValue();
}
