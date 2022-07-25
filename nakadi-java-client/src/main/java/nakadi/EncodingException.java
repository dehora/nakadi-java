package nakadi;

/**
 * An exception representing an issue encoding request data
 */
public class EncodingException extends HttpException {

  /**
   * @param problem the Problem detail
   */
  public EncodingException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public EncodingException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
