package nakadi;

/**
 * Supplied to {@link NakadiClient} and provides {@link Resource} objects that can be used
 * to make requests against the server. Acts as a {@link FunctionalInterface} and
 * can be used via a lambda expression.
 */
@FunctionalInterface
public interface ResourceProvider {

  /**
   * Supply a new resource to access the server.
   *
   * @return a new {@link Resource}
   */
  Resource newResource();
}
