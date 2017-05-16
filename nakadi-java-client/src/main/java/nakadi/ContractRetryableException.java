package nakadi;

/**
 * An exception representing a API mismatch with the server and where it's deemed safe to
 * continue.
 */
public class ContractRetryableException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public ContractRetryableException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public ContractRetryableException(Problem problem, Throwable cause) {
    super(problem, cause);
  }

}
