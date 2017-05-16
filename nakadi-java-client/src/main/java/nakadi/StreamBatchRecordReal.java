package nakadi;

import java.util.Map;
import java.util.Objects;

class StreamBatchRecordReal<T> implements StreamBatchRecord<T> {

  private final StreamBatch<T> streamBatchRecord;
  private final StreamCursorContext streamCursorContext;
  private final StreamOffsetObserver streamOffsetObserver;
  private String streamIdHeader;

  StreamBatchRecordReal(EventStreamBatch<T> batch, StreamOffsetObserver streamOffsetObserver,
      Map<String, String> context) {
    Objects.requireNonNull(batch);
    Objects.requireNonNull(streamOffsetObserver);
    Objects.requireNonNull(context);
    this.streamOffsetObserver = streamOffsetObserver;
    this.streamBatchRecord = new StreamBatchReal<>(batch);
    streamCursorContext = new StreamCursorContextReal(streamBatchRecord.cursor(), context);
  }

  StreamBatchRecordReal(EventStreamBatch<T> batch, StreamOffsetObserver streamOffsetObserver) {
    Objects.requireNonNull(batch);
    Objects.requireNonNull(streamOffsetObserver);
    this.streamOffsetObserver = streamOffsetObserver;
    this.streamBatchRecord = new StreamBatchReal<>(batch);
    streamCursorContext = new StreamCursorContextReal(streamBatchRecord.cursor());
  }

  @Override public StreamBatch<T> streamBatch() {
    return streamBatchRecord;
  }

  @Override public StreamCursorContext streamCursorContext() {
    return streamCursorContext;
  }

  @Override public StreamOffsetObserver streamOffsetObserver() {
    return streamOffsetObserver;
  }

  @Override public int hashCode() {
    return Objects.hash(streamBatchRecord, streamCursorContext, streamOffsetObserver,
        streamIdHeader);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamBatchRecordReal<?> that = (StreamBatchRecordReal<?>) o;
    return Objects.equals(streamBatchRecord, that.streamBatchRecord) &&
        Objects.equals(streamCursorContext, that.streamCursorContext) &&
        Objects.equals(streamOffsetObserver, that.streamOffsetObserver) &&
        Objects.equals(streamIdHeader, that.streamIdHeader);
  }

  @Override public String toString() {
    return "StreamBatchRecordReal{" + "streamBatchRecord=" + streamBatchRecord +
        ", streamCursorContext=" + streamCursorContext +
        ", streamOffsetObserver=" + streamOffsetObserver +
        ", streamIdHeader='" + streamIdHeader + '\'' +
        '}';
  }
}
