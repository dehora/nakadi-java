package nakadi;

/**
 * An exception representing where it's deemed not ok to retry.
 */
public class NonRetryableNakadiException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public NonRetryableNakadiException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public NonRetryableNakadiException(Problem problem, Throwable cause) {
    super(problem, cause);
  }

}
