package nakadi;

public class ServerException extends NakadiException {

  public ServerException(Problem problem) {
    super(problem);
  }

  public ServerException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
