package nakadi;

/**
 * An exception representing an error from the client. All client API exceptions extend this one.
 */
public class NakadiException extends RuntimeException {

  private Problem problem;

  /**
   * @param problem the Problem detail
   */
  public NakadiException(Problem problem) {
    super(problem.toMessage());
    this.problem = problem;
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public NakadiException(Problem problem, Throwable cause) {
    super(problem.toMessage(), cause);
  }

  /**
   * Throw an IllegalArgumentException if the argument is null.
   *
   * @param arg the object to
   * @param message the exception message
   * @return the object if not null
   * @throws IllegalArgumentException if {@code obj} is {@code null}
   */
  public static <T> T throwNonNull(T arg, String message) {
    if (arg == null) {
      throw new IllegalArgumentException(message);
    }
    return arg;
  }

  /**
   * @return the problem detail
   */
  public Problem problem() {
    return problem;
  }
}
