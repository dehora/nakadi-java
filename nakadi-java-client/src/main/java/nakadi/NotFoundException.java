package nakadi;

/**
 * An exception representing a 404 response code.
 */
public class NotFoundException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public NotFoundException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public NotFoundException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
