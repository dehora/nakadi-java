package nakadi;

/**
 * An exception representing a 409 Conflict response code.
 */
public class ConflictException extends HttpException {

  /**
   * @param problem the Problem detail
   */
  public ConflictException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public ConflictException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
