package nakadi;

/**
 * An exception representing a http failure.
 */
public class HttpException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public HttpException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public HttpException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
