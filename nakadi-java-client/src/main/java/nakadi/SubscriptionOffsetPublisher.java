package nakadi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Unstable
public class SubscriptionOffsetPublisher implements StreamOffsetObserver {
  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setUncaughtExceptionHandler(
              (t, e) -> logger.error("stream_observer_err {}, {}", t, e.getMessage(), e))
          .setNameFormat("nakadi-java-stream-observer-%d").build());

  private final PublishProcessor<StreamCursorContext> processor;
  private final Flowable<StreamCursorContext> flowable;

  private SubscriptionOffsetPublisher() {
    processor = PublishProcessor.create();
    flowable = Flowable
        .fromPublisher(processor)
        .observeOn(Schedulers.from(EXECUTOR));
  }

  static SubscriptionOffsetPublisher create() {
    return new SubscriptionOffsetPublisher();
  }

  @Override public void onNext(StreamCursorContext streamCursorContext) throws NakadiException {
    processor.onNext(streamCursorContext);
  }

  SubscriptionOffsetPublisher subscribe(StreamOffsetObserver streamOffsetObserver) {
    flowable.subscribe(new StreamCursorContextSubscriber(streamOffsetObserver));
    return this;
  }

  void onComplete() {
    processor.onComplete();
  }

  class StreamCursorContextSubscriber implements Subscriber<StreamCursorContext> {

    final StreamOffsetObserver streamOffsetObserver;

    StreamCursorContextSubscriber(StreamOffsetObserver streamOffsetObserver) {
      this.streamOffsetObserver = streamOffsetObserver;
    }

    @Override public void onSubscribe(Subscription s) {
      s.request(Long.MAX_VALUE);
    }

    @Override public void onNext(StreamCursorContext streamCursorContext) {
      streamOffsetObserver.onNext(streamCursorContext);
    }

    @Override public void onError(Throwable t) {
      logger.error("SubscriptionOffsetPublisher.subscriber onError " + t.getMessage(), t);
    }

    @Override public void onComplete() {
      logger.info("SubscriptionOffsetPublisher.subscriber onComplete ");
    }
  }
}
