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

  static final URI T1000_TYPE = URI.create("about:t1000");
  private static final URI DEFAULT_TYPE = URI.create("about:blank");
  private static final URI LOCAL_TYPE = URI.create("about:local");
  private static final URI AUTH_TYPE = URI.create("about:auth");
  private static final URI CONTRACT_TYPE = URI.create("about:contract");
  private static final URI CONTRACT_RETRYABLE_TYPE = URI.create("about:contract_retryable");
  private static final URI OBSERVER_TYPE = URI.create("about:observer");
  private static final URI NETWORK_TYPE = URI.create("about:wire");
  private static final URI MARSHAL_ENTITY_TYPE = URI.create("about:marshal_entity");
  private static final Map SENTINEL_MAP = new HashMap();
  public static final int MAX_RAW_ERR_LEN = 200;
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
   * Quick way to create a Problem object that indicates the request had an auth issue.
   *
   * @param title the problem title
   * @param detail the problem detail
   * @return a Problem object with a status of 401 and a type of "about:auth"
   */
  public static Problem authProblem(String title, String detail) {
    return new Problem()
        .title(title)
        .detail(detail)
        .data(SENTINEL_MAP)
        .status(401)
        .type(AUTH_TYPE);
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

  /**
   * Quick way to create a problem that indicates a mismatch in client expectations about what
   * the server is responding with, and is considered safe to continue.
   *
   * @param title the problem title
   * @param detail the problem detail
   * @return a Problem object with a status of 500 and a type of "about:contract"
   */
  public static Problem contractRetryableProblem(String title, String detail) {
    return new Problem()
        .title(title)
        .detail(detail)
        .data(SENTINEL_MAP)
        .status(500)
        .type(CONTRACT_RETRYABLE_TYPE);
  }

  /**
   * Quick way to create a Problem object that indicates a stream observer throw an exception.
   *
   * @param title the problem title
   * @param detail the problem detail
   * @return a Problem object with a status of 500 and a type of "about:observer"
   */
  public static Problem observerProblem(String title, String detail) {
    return new Problem()
        .title(title)
        .detail(detail)
        .data(SENTINEL_MAP)
        .status(400)
        .type(OBSERVER_TYPE);
  }

  /**
   * Quick way to create a Problem object that indicates a server responded with an error but
   * the message could not be converted to a Problem object automatically; typically this happens
   * when a server responds with a non-json payload.
   *
   * @param statusCode the HTTP status code of the server's response
   * @param body the HTTP body of the server's response
   * @param errorDetail details about why it failed to convert the server's response to a Problem
   * @return a Problem with a status of {@code statusCode} and a type of "about:marshal_entity"
   */
  public static Problem rawProblem(int statusCode, String body, String errorDetail) {
    return new Problem()
            .title(body.substring(0, Math.min(MAX_RAW_ERR_LEN, body.length())))
            .detail(errorDetail)
            .data(SENTINEL_MAP)
            .status(statusCode)
            .type(MARSHAL_ENTITY_TYPE);
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
