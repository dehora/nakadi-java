package nakadi;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Problem, either sent by the server as Problem JSON, or created locally.
 */
public class Problem {

  private static final URI DEFAULT_TYPE = URI.create("about:blank");
  private static final URI LOCAL_TYPE = URI.create("about:local");
  static final URI T1000_TYPE = URI.create("about:t1000");
  private static final URI CONTRACT_TYPE = URI.create("about:contract");
  private static final URI NETWORK_TYPE = URI.create("about:wire");
  private static final Map SENTINEL_MAP = new HashMap();
  private URI type = DEFAULT_TYPE;
  private String title;
  private int status;
  private String detail;
  private URI instance;
  private Map data;
  private volatile transient String message;

  /**
   * Quick way to create a Problem object that indicates the request never
   * left the client.
   *
   * @param title the problem title
   * @param detail the problem detail
   * @return a Problem object with a status of 400 and a type of "about:local"
   */
  public static Problem localProblem(String title, String detail) {
    return new Problem()
        .title(title)
        .detail(detail)
        .data(SENTINEL_MAP)
        .status(400)
        .type(LOCAL_TYPE);
  }

  /**
   * Quick way to create a Problem object that indicates the server never
   * sent a problem.
   *
   * @param title the problem title
   * @param detail the problem detail
   * @return a Problem object with a status of code and a type of "about:t1000"
   */
  public static Problem noProblemo(String title, String detail, int code) {
    return new Problem()
        .title(title)
        .detail(detail)
        .data(SENTINEL_MAP)
        .status(code)
        .type(T1000_TYPE);
  }

  /**
   * Quick way to create a problem that indicates an underlying network issue; typically this
   * means we couldn't get onto the network at all.
   *
   * @param title the problem title
   * @param detail the problem detail
   * @return a Problem object with a status of 400 and a type of "about:network"
   */
  public static Problem networkProblem(String title, String detail) {
    return new Problem()
        .title(title)
        .detail(detail)
        .data(SENTINEL_MAP)
        .status(400)
        .type(NETWORK_TYPE);
  }

  /**
   * Quick way to create a problem that indicates a mismatch in client expectations about what
   * the server is responding with such that it might not be safe to continue.
   *
   * @param title the problem title
   * @param detail the problem detail
   * @return a Problem object with a status of 500 and a type of "about:contract"
   */
  public static Problem contractProblem(String title, String detail) {
    return new Problem()
        .title(title)
        .detail(detail)
        .data(SENTINEL_MAP)
        .status(500)
        .type(CONTRACT_TYPE);
  }

  public String toMessage() {
    if (message != null) {
      return message;
    }
    message = title() + "; " + detail().orElse("") +
        " (" + status() + ")";
    return message;
  }

  public URI type() {
    return type;
  }

  public Problem type(URI type) {
    this.type = type;
    return this;
  }

  public String title() {
    return title;
  }

  public Problem title(String title) {
    this.title = title;
    return this;
  }

  public int status() {
    return status;
  }

  public Problem status(int status) {
    this.status = status;
    return this;
  }

  public Optional<String> detail() {
    return Optional.ofNullable(detail);
  }

  public Problem detail(String detail) {
    this.detail = detail;
    return this;
  }

  public Optional<URI> instance() {
    return Optional.ofNullable(instance);
  }

  public Problem instance(URI instance) {
    this.instance = instance;
    return this;
  }

  public Map data() {
    return data;
  }

  public Problem data(Map data) {
    this.data = data;
    return this;
  }

  @Override public int hashCode() {
    return Objects.hash(type, title, status, detail, instance, data);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Problem problem = (Problem) o;
    return status == problem.status &&
        Objects.equals(type, problem.type) &&
        Objects.equals(title, problem.title) &&
        Objects.equals(detail, problem.detail) &&
        Objects.equals(instance, problem.instance) &&
        Objects.equals(data, problem.data);
  }

  @Override public String toString() {
    return "Problem{" + "type=" + type +
        ", title='" + title + '\'' +
        ", status=" + status +
        ", detail='" + detail + '\'' +
        ", instance=" + instance +
        ", data=" + data +
        '}';
  }
}
