package nakadi;

/**
 * An exception representing a 422 Invalid response code.
 */
public class InvalidException extends HttpException {

  /**
   * @param problem the Problem detail
   */
  public InvalidException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public InvalidException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
