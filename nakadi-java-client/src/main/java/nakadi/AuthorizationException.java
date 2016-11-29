package nakadi;

/**
 * An exception representing a 401 Unauthorized response code.
 */
public class AuthorizationException extends HttpException {

  /**
   * @param problem the Problem detail
   */
  public AuthorizationException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public AuthorizationException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
