package nakadi;

public class NetworkException extends NakadiException {

  public NetworkException(Problem problem) {
    super(problem);
  }

  public NetworkException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
