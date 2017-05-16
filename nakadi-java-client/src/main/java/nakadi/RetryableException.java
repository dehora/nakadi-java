package nakadi;

/**
 * An exception representing where it's deemed ok to retry.
 */
public class RetryableException extends NakadiException {

  /**
   * @param cause the cause
   */
  public RetryableException(Throwable cause) {
    this(Problem.observerProblem("retryable_error", cause.getMessage()), cause);
  }

  /**
   * @param problem the Problem detail
   */
  public RetryableException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public RetryableException(Problem problem, Throwable cause) {
    super(problem, cause);
  }

  /**
   * Create and throw a RetryableException with an underlying cause.
   *
   * @param cause the cause
   */
  public static void throwing(Throwable cause) {
    throw new RetryableException(cause);
  }
}
