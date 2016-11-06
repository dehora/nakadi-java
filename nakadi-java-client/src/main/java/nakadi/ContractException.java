package nakadi;

public class ContractException extends NakadiException {

  public ContractException(Problem problem) {
    super(problem);
  }

  public ContractException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
