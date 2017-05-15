package nakadi;

/**
 * An exception representing an error from an observer that should result in explicitly
 * stopping the underlying {@link StreamProcessor} from retrying.
 */
@Unstable
public class ObserverCrash extends NakadiException {

  /**
   * Create and throw an ObserverCrash with an underlying cause.
   *
   * @param cause the cause
   */
  public static void throwing(Throwable cause) {
    throw new ObserverCrash(cause);
  }

  /**
   * @param cause the cause
   */
  public ObserverCrash(Throwable cause) {
    this(Problem.observerProblem("observer_crash", cause.getMessage()), cause);
  }

  /**
   * @param problem the Problem detail
   */
  public ObserverCrash(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public ObserverCrash(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
