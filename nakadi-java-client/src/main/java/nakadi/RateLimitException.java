package nakadi;

/**
 * An exception representing a 429 Too Many Requests response code.
 */
public class RateLimitException extends HttpException {

  /**
   * @param problem the Problem detail
   */
  public RateLimitException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public RateLimitException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
