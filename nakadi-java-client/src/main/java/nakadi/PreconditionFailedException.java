package nakadi;

/**
 * An exception representing a 412 Precondition Failed response code.
 */
public class PreconditionFailedException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public PreconditionFailedException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public PreconditionFailedException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
