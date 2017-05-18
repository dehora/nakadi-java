package nakadi;

import java.util.concurrent.TimeUnit;

/**
 * The {@link MetricCollector} allows metrics to be captured and emitted to an implementation.
 *
 * <p>
 * There are two kinds of metric:
 * </p>
 * <ul>
 * <li><code>Meter</code>: records discrete occurrences as a number.</li>
 * <li><code>Timer</code>: records durations based on a {@link TimeUnit}.</li>
 * </ul>
 *
 *
 * <p>
 * Each emitted metric is an enum. Implementations can look at the enum and record as they wish.
 * They can also work with them generally and ask any enum for its path, which will be a dotted
 * string.
 * </p>
 */
public interface MetricCollector {

  /**
   * Mark the single occurrence of a given metric.
   *
   * @param meter the metric
   */
  void mark(MetricCollector.Meter meter);

  /**
   * Mark on one or more occurrences of a given metric.
   *
   * @param meter the metric
   * @param count the number of occurrences
   */
  void mark(MetricCollector.Meter meter, long count);

  /**
   * Mark the duration of a given metric.
   * <p></p>
   * Implementations should pre-process the duration using the unit with their preferred unit of
   * measure, eg, <code>unit.toNanos(duration)</code>.
   *
   * @param timer the metric
   * @param duration how long the occurrence took
   * @param unit the occurrence's time unit.
   */
  void duration(MetricCollector.Timer timer, long duration, TimeUnit unit);

  /**
   * A metric that measures an occurrence.
   */
  enum Meter {
    /**
     * Each time the overall event stream processor is restarted.
     */
    streamRestart("nakadi.java.client.stream.event.restart"),

    /**
     * Each time an event stream or subscription processor connection is retried.
     */
    consumerRetry("nakadi.java.client.stream.consumer.retry"),

    /**
     * Each time an event is consumed from the stream.
     */
    received("nakadi.java.client.event.received"),

    /**
     * Each time a batch is consumed from the stream.
     */
    receivedBatch("nakadi.java.client.event.receivedBatch"),

    /**
     * Each time a batch keepalive is consumed from the stream.
     */
    receivedKeepalive("nakadi.java.client.event.receivedKeepalive"),

    /**
     * Each time the server rejects a checkpoint due to a bad session id.
     */
    sessionCheckpointMismatch("nakadi.java.client.event.sessionCheckpointMismatch"),

    /**
     * Each time the server rejects a checkpoint due to a network timeout.
     */
    sessionCheckpointNetworkException("nakadi.java.client.event.sessionCheckpointNetworkException"),

    /**
     * Each time an unknown error response is seen.
     */
    httpUnknown("nakadi.java.client.http.error"),

    /**
     * Each time a 207 response is seen.
     */
    http207("nakadi.java.client.http.207"),

    /**
     * Each time a 4xx response is seen.
     */
    http4xx("nakadi.java.client.http.4xx"),

    /**
     * Each time a 400 response is seen.
     */
    http400("nakadi.java.client.http.400"),

    /**
     * Each time a 401 response is seen.
     */
    http401("nakadi.java.client.http.401"),

    /**
     * Each time a 403 response is seen.
     */
    http403("nakadi.java.client.http.403"),

    /**
     * Each time a 404 response is seen.
     */
    http404("nakadi.java.client.http.404"),

    /**
     * Each time an event is sent.
     */
    sent("nakadi.java.client.event.sent"),

    /**
     * Each time a 429 response is seen.
     */
    http429("nakadi.java.client.http.429"),

    /**
     * Each time a 409 response is seen.
     */
    http409("nakadi.java.client.http.409"),

    /**
     * Each time a 412 response is seen.
     */
    http412("nakadi.java.client.http.412"),

    /**
     * Each time a 422 response is seen.
     */
    http422("nakadi.java.client.http.422"),

    /**
     * Each time a 5xx response is seen.
     */
    http5xx("nakadi.java.client.http.5xx"),

    /**
     * Each time a 500 response is seen.
     */
    http500("nakadi.java.client.http.500"),

    /**
     * Each time a 503 response is seen.
     */
    http503("nakadi.java.client.http.503"),

    /**
     * Each time a {@link RetryPolicy} is skipped because it's already finished
     */
    retrySkipFinished("nakadi.java.client.retry.skip_finished"),;

    private final String path;

    Meter(String path) {
      this.path = path;
    }

    /**
     * The distinct path for the metric restricted to a dotted string. Useful for sending
     * into downstream metric collectors.
     */
    public String path() {
      return path;
    }
  }

  /**
   * A metric that measures a duration of time.
   */
  enum Timer {

    /**
     * How long it took to send an event.
     */
    eventSend("nakadi.java.client.event.sendtime"),

    /**
     * How long it took to post a checkpoint.
     */
    checkpointSend("nakadi.java.client.checkpoint.sendtime"),

    /**
     * How long it took to reset cursor offsets.
     */
    cursorReset("nakadi.java.client.cursor.reset.sendtime"),
    ;

    private final String path;

    Timer(String path) {
      this.path = path;
    }

    /**
     * The distinct path for the metric restricted to a dotted string. Useful for sending
     * into downstream metric collectors.
     */
    public String path() {
      return path;
    }
  }
}
