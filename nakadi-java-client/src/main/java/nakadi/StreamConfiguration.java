package nakadi;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Configure the connection used to consume batches from the server.
 */
public class StreamConfiguration {

  static final int DEFAULT_BATCH_LIMIT = 1;
  static final int DEFAULT_STREAM_LIMIT = 0;
  static final int DEFAULT_BATCH_FLUSH_TIMEOUT = 30;
  static final int DEFAULT_STREAM_TIMEOUT = 0;
  static final int DEFAULT_STREAM_KEEPALIVE_COUNT = 0;
  static final int DEFAULT_MAX_UNCOMMITTED_EVENTS = 10;

  // api declared
  private int batchLimit = DEFAULT_BATCH_LIMIT;
  private int streamLimit = DEFAULT_STREAM_LIMIT;
  private long batchFlushTimeout = DEFAULT_BATCH_FLUSH_TIMEOUT;
  private long streamTimeout = DEFAULT_STREAM_TIMEOUT;
  private int streamKeepAliveLimit = DEFAULT_STREAM_KEEPALIVE_COUNT;
  private List<Cursor> cursors;
  private String topic;
  // sub api
  private String subscriptionId;
  private long maxUncommittedEvents = DEFAULT_MAX_UNCOMMITTED_EVENTS;
  // local settings
  private long connectTimeout = 30_000L;
  private long readTimeout = 60_000L;
  private long maxRetryDelay = StreamConnectionRetry.DEFAULT_MAX_DELAY_SECONDS;
  private int maxRetryAttempts = StreamConnectionRetry.DEFAULT_MAX_ATTEMPTS;

  public String eventTypeName() {
    return topic;
  }

  public StreamConfiguration eventTypeName(String topic) {
    this.topic = topic;
    return this;
  }

  public String subscriptionId() {
    return subscriptionId;
  }

  public StreamConfiguration subscriptionId(String subscriptionId) {
    this.subscriptionId = subscriptionId;
    return this;
  }

  public long connectTimeoutMillis() {
    return connectTimeout;
  }

  /**
   * Sets the default connect timeout for new connections. If 0, no timeout, otherwise
   * values must be between 1 and {@link Integer#MAX_VALUE}. The default is 30s.
   */
  public StreamConfiguration connectTimeout(long timeout, TimeUnit unit) {
    this.connectTimeout = unit.toMillis(timeout);
    return this;
  }

  public long readTimeoutMillis() {
    return readTimeout;
  }

  /**
   * Sets the default read timeout for connections. If 0, no timeout, otherwise
   * values must be between 1 and {@link Integer#MAX_VALUE}. The default is 60s.
   *
   * If you set this and {@link #batchLimit()} is 0, the stream may be disconnected by the client
   * before messages are released by the server.
   *
   * If you set this lower than {@link #batchFlushTimeoutSeconds()} the stream may be disconnected
   * by the client before messages are released by the server. The default of 60s is higher than
   * the {@link #batchFlushTimeoutSeconds()} of 30s for this reason.
   */
  public StreamConfiguration readTimeout(long timeout, TimeUnit unit) {
    this.readTimeout = unit.toMillis(timeout);
    return this;
  }

  public long maxUncommittedEvents() {
    return maxUncommittedEvents;
  }

  public StreamConfiguration maxUncommittedEvents(long maxUncommittedEvents) {
    this.maxUncommittedEvents = maxUncommittedEvents;
    return this;
  }

  public int batchLimit() {
    return batchLimit;
  }

  /**
   * Maximum number of Events in each batch of the stream. If 0 or unspecified
   * will buffer Events indefinitely and flush on reaching of {@link #batchFlushTimeoutSeconds()}.
   */
  public StreamConfiguration batchLimit(int batchLimit) {
    this.batchLimit = batchLimit;
    return this;
  }

  public int streamLimit() {
    return streamLimit;
  }

  /**
   * Set the maximum number of Events in this stream (over all partitions being streamed in this
   * connection). If 0 or undefined, will stream batches indefinitely. The default is 0.
   *
   * Stream initialization will fail if this limit is lower than {@link #batchLimit()}.
   */
  public StreamConfiguration streamLimit(int streamLimit) {
    this.streamLimit = streamLimit;
    return this;
  }

  public long batchFlushTimeoutSeconds() {
    return batchFlushTimeout;
  }

  /**
   * Set the maximum time in seconds to wait for the flushing of each chunk (per partition). The
   * default is 30s. If the amount of buffered Events reaches {@link #batchLimit()} before this
   * timeout is reached, the messages are immediately flushed to the client and batch flush timer
   * is reset.
   */
  public StreamConfiguration batchFlushTimeout(long batchFlushTimeout, TimeUnit unit) {
    this.batchFlushTimeout = unit.toSeconds(batchFlushTimeout);
    return this;
  }

  public long streamTimeoutSeconds() {
    return streamTimeout;
  }

  /**
   * Set the maximum time (in seconds) a stream will live before connection is closed by the
   * server. If 0 or unspecified will stream indefinitely. The default is 0.
   *
   * Stream initialization will fail if this is lower than {@link #batchFlushTimeoutSeconds()}.
   */
  public StreamConfiguration streamTimeout(long streamTimeout, TimeUnit unit) {
    this.streamTimeout = unit.toSeconds(streamTimeout);
    return this;
  }

  public int streamKeepAliveLimit() {
    return streamKeepAliveLimit;
  }

  /**
   * Set the maximum number of empty keep alive batches to get in a row before closing
   * the connection. If 0 or undefined will send keep alive messages indefinitely. Default 0.
   */
  public StreamConfiguration streamKeepAliveLimit(int streamKeepAliveLimit) {
    this.streamKeepAliveLimit = streamKeepAliveLimit;
    return this;
  }

  public Optional<List<Cursor>> cursors() {
    return Optional.ofNullable(cursors);
  }

  /**
   * Cursors indicating the partitions to read from and the starting offsets. If this is left
   * empty all partitions are consumed.
   *
   * todo: how are these spread out across threads?
   */
  public StreamConfiguration cursors(Cursor... cursors) {
    NakadiException.throwNonNull(cursors, "Please provide at least one cursor");
    if (this.cursors == null) {
      this.cursors = Lists.newArrayList();
    }
    this.cursors.addAll(Arrays.asList(cursors));
    return this;
  }

  /**
   * Cursor indicating the partitions to read from and the starting offsets.  If this is left
   * empty all partitions are consumed.
   *
   * Calling this repeatedly is the same as calling cursors (ie, you add more cursors
   * instead of replacing the existing one).
   *
   * todo: how are these spread out across threads?
   */
  public StreamConfiguration cursor(Cursor cursor) {
    NakadiException.throwNonNull(cursor, "Please provide a cursor");
    return cursors(cursor);
  }

  public long maxRetryDelaySeconds() {
    return maxRetryDelay;
  }

  public StreamConfiguration maxRetryDelay(long maxRetryDelay, TimeUnit unit) {
    NakadiException.throwNonNull(unit, "Please provide a time unit for max retry delay");
    this.maxRetryDelay = unit.toSeconds(maxRetryDelay);
    ;
    return this;
  }

  public int maxRetryAttempts() {
    return maxRetryAttempts;
  }

  public StreamConfiguration maxRetryAttempts(int maxRetryAttempts) {
    this.maxRetryAttempts = maxRetryAttempts;
    return this;
  }

  boolean isSubscriptionStream() {
    return this.subscriptionId() != null;
  }

  boolean isEventTypeStream() {
    return this.eventTypeName() != null;
  }

  @Override public int hashCode() {
    return Objects.hash(batchLimit, streamLimit, batchFlushTimeout, streamTimeout,
        streamKeepAliveLimit, cursors, connectTimeout, readTimeout);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamConfiguration that = (StreamConfiguration) o;
    return batchLimit == that.batchLimit &&
        streamLimit == that.streamLimit &&
        batchFlushTimeout == that.batchFlushTimeout &&
        streamTimeout == that.streamTimeout &&
        streamKeepAliveLimit == that.streamKeepAliveLimit &&
        connectTimeout == that.connectTimeout &&
        readTimeout == that.readTimeout &&
        Objects.equals(cursors, that.cursors);
  }

  @Override public String toString() {
    return "StreamConfiguration{" + "batchLimit=" + batchLimit +
        ", streamLimit=" + streamLimit +
        ", batchFlushTimeout=" + batchFlushTimeout +
        ", streamTimeout=" + streamTimeout +
        ", streamKeepAliveLimit=" + streamKeepAliveLimit +
        ", cursors=" + cursors +
        ", connectTimeoutMillis=" + connectTimeout +
        ", readTimeoutMillis=" + readTimeout +
        '}';
  }
}
