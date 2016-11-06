package nakadi;

public class ClientException extends NakadiException {

  public ClientException(Problem problem) {
    super(problem);
  }

  public ClientException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
