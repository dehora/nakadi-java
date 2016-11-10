package nakadi;

/**
 * An exception representing a API mismatch with the server and where it's deemed not safe to
 * continue.
 */
public class ContractException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public ContractException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public ContractException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
