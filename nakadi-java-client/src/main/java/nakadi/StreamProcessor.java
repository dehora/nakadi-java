package nakadi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import java.io.BufferedReader;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API support for streaming events to a consumer. <p> Supports both the name event type streams and
 * the more recent subscription based streams. The API's connection streaming models are
 * fundamentally the same, but have some differences in  detail, such as request parameters and the
 * structure of the batch cursor. Users should consult the Nakadi API documentation and {@link
 * StreamConfiguration} for details on the streaming options. </p>
 *
 * @see nakadi.StreamConfiguration
 */
public class StreamProcessor implements StreamProcessorManaged {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final String X_NAKADI_STREAM_ID = "X-Nakadi-StreamId";
  private static final int DEFAULT_HALF_OPEN_CONNECTION_GRACE_SECONDS = 90;
  private static final int DEFAULT_BACKPRESSURE_BUFFER_SIZE = 8000;
  private final NakadiClient client;
  private final StreamConfiguration streamConfiguration;
  private final StreamObserverProvider streamObserverProvider;
  private final StreamOffsetObserver streamOffsetObserver;
  private final JsonBatchSupport jsonBatchSupport;
  private final long maxRetryDelay;
  private final int maxRetryAttempts;
  // non builder supplied
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicBoolean stopping = new AtomicBoolean(false);
  private final CountDownLatch startLatch;
  private final StreamProcessorRequestFactory streamProcessorRequestFactory;
  private volatile Throwable failedProcessorException;
  private StreamBatchRecordSubscriber subscriber;
  private final ExecutorService monoIoExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setNameFormat("nakadi-java-io-%d")
          .setUncaughtExceptionHandler((t, e) -> handleUncaught(t, e, "stream_processor_err_io"))
          .build());
  private final Scheduler monoIoScheduler = Schedulers.from(monoIoExecutor);
  private final ExecutorService monoComputeExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setNameFormat("nakadi-java-compute-%d")
          .setUncaughtExceptionHandler(
              (t, e) -> handleUncaught(t, e, "stream_processor_err_compute"))
          .build());
  private final Scheduler monoComputeScheduler = Schedulers.from(monoComputeExecutor);

  @VisibleForTesting
  @SuppressWarnings("unused") StreamProcessor(NakadiClient client,
      StreamProcessorRequestFactory streamProcessorRequestFactory) {
    this.client = client;
    this.streamConfiguration = null;
    this.streamObserverProvider = null;
    this.streamOffsetObserver = null;
    this.jsonBatchSupport = new JsonBatchSupport(client.jsonSupport());
    this.maxRetryDelay = StreamConnectionRetryFlowable.DEFAULT_MAX_DELAY_SECONDS;
    this.maxRetryAttempts = StreamConnectionRetryFlowable.DEFAULT_MAX_ATTEMPTS;
    startLatch = new CountDownLatch(1);
    this.streamProcessorRequestFactory = streamProcessorRequestFactory;
  }

  private StreamProcessor(Builder builder) {
    this.streamConfiguration = builder.streamConfiguration;
    this.client = builder.client;
    this.streamObserverProvider = builder.streamObserverProvider;
    this.streamOffsetObserver = builder.streamOffsetObserver;
    this.jsonBatchSupport = new JsonBatchSupport(client.jsonSupport());
    this.maxRetryDelay = streamConfiguration.maxRetryDelaySeconds();
    this.maxRetryAttempts = streamConfiguration.maxRetryAttempts();
    startLatch = new CountDownLatch(1);
    this.streamProcessorRequestFactory = builder.streamProcessorRequestFactory;
  }

  /**
   * Provide a new builder for creating a stream processor.
   *
   * @param client the client
   * @return a builder
   */
  public static StreamProcessor.Builder newBuilder(NakadiClient client) {
    return new StreamProcessor.Builder().client(client);
  }

  /**
   * Start consuming the stream. <p> This runs in a background executor and will not block the
   * calling thread. Callers must hold onto a reference in order to be able to shut it down.</p>
   *
   * <p> Calling start multiple times is the same as calling it once, when stop is not also called
   * or interleaved with. </p>
   *
   * @throws IllegalStateException if the processor has already been stopped.
   * @see #stop()
   */
  public void start() throws IllegalStateException {
    if (stopped() || stopping()) {
      throw new IllegalStateException("processor has already been stopped and cannot be restarted");
    }

    setupRxErrorHandler();

    if (!started.getAndSet(true)) {
      this.startStreaming();
    }

    waitingOnStart();
  }

  /**
   * Perform a controlled shutdown of the stream. <p> The {@link StreamObserver} will have its
   * onCompleted or onError called under normal circumstances. The {@link StreamObserver} in turn
   * can call its {@link StreamOffsetObserver} to perform cleanup.</p>
   *
   * <p> Calling stop multiple times is the same as calling it once, when start is not also called
   * or interleaved with. </p>
   *
   * @see #start()
   */
  public void stop() {

    waitingOnStart();

    if (started.getAndSet(false)) {
      stopStreaming();
    }
  }

  /**
   * Indicates if the processor is running. <p> This is set after {@link #start()} is called. Note
   * that it can be true when the processor is setting up and before it actually consumes the stream
   * from Nakadi. After calling {@link #stop()} or after an error that caused the processor to stop
   * working this will be false. </p>
   *
   * @return true if running, false if not.
   */
  public boolean running() {
    return !stopping() && !stopped() & started();
  }

  /**
   * Allows tracking of errors from StreamProcessor. <p> If the processor stopped due to an error
   * that error will be visible from this method. If there's no error this return {@link
   * Optional#empty()}. </p>
   *
   * @return the exception that stopped the processor, or {@link Optional#empty()}
   */
  @SuppressWarnings("WeakerAccess") public Optional<Throwable> failedProcessorException() {
    return Optional.ofNullable(failedProcessorException);
  }

  boolean stopped() {
    return stopped.get();
  }

  boolean started() {
    return started.get();
  }

  private boolean stopping() {
    return stopped.get() || stopping.get();
  }

  private void startStreaming() {
    //noinspection unchecked
    stream(streamConfiguration, streamObserverProvider);
    startLatch.countDown();
  }

  private void waitingOnStart() {
    try {
      startLatch.await(60, TimeUnit.SECONDS);
      logger.info("stream_processor op=has_started");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void stopStreaming() {
    stopping.getAndSet(true);
    subscriber.dispose();
    logger.debug("stream_processor op=stopping_executor name=monoIoScheduler");
    ExecutorServiceSupport.shutdown(monoIoExecutor);
    logger.debug("stream_processor op=stopping_executor name=monoComputeScheduler");
    ExecutorServiceSupport.shutdown(monoComputeExecutor);
    stopped.getAndSet(true);
  }

  private <T> void stream(StreamConfiguration sc, StreamObserverProvider<T> provider) {

    final StreamObserver<T> observer = provider.createStreamObserver();
    final TypeLiteral<T> literal = provider.typeLiteral();
    final Flowable<StreamBatchRecord<T>> observable = this.buildObservable(observer, sc, literal);

    // Do processing on monoComputeScheduler; if the monoIoScheduler (or any shared
    // single thread executor) is used, the pipeline can lock up as the thread is dominated by
    // io and never frees to process batches. monoComputeScheduler is a single thread executor
    // to make things easier to reason about for now wrt to ordering/sequential batch processing
    // (but the regular computation scheduler could work as well maybe).
    Optional<Integer> maybeBuffering = observer.requestBuffer();
    if (maybeBuffering.isPresent()) {
      logger.info("op=create_subscriber type=buffering buffer={} config={}", sc);

      observable.observeOn(monoComputeScheduler)
          // If the stream observer wants buffering set that up; it will see discrete
          // batches but the rx observer wrapping around it will be given buffered lists.
          .buffer(maybeBuffering.get())
          .subscribeWith(
              new StreamBatchRecordBufferingSubscriber<>(observer, client.metricCollector()));
    } else {
      logger.info("op=create_subscriber type=regular config={}", sc);
      subscriber = new StreamBatchRecordSubscriber<>(observer, client.metricCollector());
      observable.observeOn(monoComputeScheduler)
          .subscribeWith(new StreamBatchRecordSubscriber<>(observer, client.metricCollector()));
    }
  }

  private <T> Flowable<StreamBatchRecord<T>> buildObservable(
      StreamObserver<T> streamObserver,
      StreamConfiguration streamConfiguration,
      TypeLiteral<T> typeLiteral) {

    // compute a timeout after which we assume the server's gone away or we're on
    // one end of a half-open connection. this is a big downside trying to emulate
    // a streaming model over http get
    TimeUnit halfOpenUnit = TimeUnit.SECONDS;
    long halfOpenGrace = DEFAULT_HALF_OPEN_CONNECTION_GRACE_SECONDS;
    long batchFlushTimeoutSeconds = this.streamConfiguration.batchFlushTimeoutSeconds();
    long halfOpenKick = halfOpenUnit.toSeconds(batchFlushTimeoutSeconds + halfOpenGrace);
    logger.info(
        "op=processor_configure_half_open, batch_flush_timeout={}, grace_period={}, kick_after={}{}",
        batchFlushTimeoutSeconds, halfOpenGrace, halfOpenKick, halfOpenUnit.name().toLowerCase());

    // monoIoScheduler: okhttp needs to be closed on the same thread that opened it; using a
    // single thread scheduler allows that to happen whereas the default/io/compute schedulers
    // all use multiple threads which can cause resource leaks: http://bit.ly/2fe4UZH
    final Flowable<StreamBatchRecord<T>> flowable = Flowable.using(
        httpRequestFactory(streamConfiguration),
        streamConsumerFactory(typeLiteral, streamConfiguration),
        httpResponseDispose()
    )
        .subscribeOn(monoIoScheduler)
        .unsubscribeOn(monoIoScheduler)
        .doOnSubscribe(subscription -> streamObserver.onStart())
        .doOnComplete(streamObserver::onCompleted)
        .doOnCancel(streamObserver::onStop)
        .timeout(halfOpenKick, halfOpenUnit)
        // retries handle issues like network failures and 409 conflicts
        .retryWhen(buildStreamConnectionRetryFlowable())
        // restarts handle when the server closes the connection (eg checkpointing fell behind)
        .compose(buildRestartHandler())
        .onBackpressureBuffer(DEFAULT_BACKPRESSURE_BUFFER_SIZE, true, true);

    return Flowable.defer(() -> flowable);
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Callable<Response> httpRequestFactory(StreamConfiguration sc) {
    return streamProcessorRequestFactory.createCallable(sc, this);
  }

  private <T> Function<? super Response, Flowable<StreamBatchRecord<T>>> streamConsumerFactory(
      TypeLiteral<T> literal, StreamConfiguration sc) {

    return (Response response) -> {
      final BufferedReader br = new BufferedReader(response.responseBody().asReader());
      return Flowable.fromIterable(br.lines()::iterator)
          .doOnError(throwable -> ResponseSupport.closeQuietly(response))
          .onBackpressureBuffer(DEFAULT_BACKPRESSURE_BUFFER_SIZE, true, true)
          .map(r -> lineToStreamBatchRecord(r, literal, response, sc));
    };
  }

  private Consumer<? super Response> httpResponseDispose() {
    return ResponseSupport::closeQuietly;
  }

  private StreamConnectionRetryFlowable buildStreamConnectionRetryFlowable() {

    final ExponentialRetry exponentialRetry = ExponentialRetry.newBuilder()
        .initialInterval(
            StreamConnectionRetryFlowable.DEFAULT_INITIAL_DELAY_SECONDS,
            StreamConnectionRetryFlowable.DEFAULT_TIME_UNIT
        )
        .maxInterval(maxRetryDelay, StreamConnectionRetryFlowable.DEFAULT_TIME_UNIT)
        .maxAttempts(maxRetryAttempts)
        .build();

    return new StreamConnectionRetryFlowable(
        exponentialRetry, buildRetryFunction(), client.metricCollector(), this);
  }

  private <T> FlowableTransformer<StreamBatchRecord<T>, StreamBatchRecord<T>> buildRestartHandler() {

    return new StreamConnectionRestart()
        .repeatWhenWithDelayAndUntil(
            stopRepeatingPredicate(),
            StreamConnectionRestart.DEFAULT_DELAY_SECONDS,
            StreamConnectionRestart.DEFAULT_DELAY_UNIT,
            StreamConnectionRestart.DEFAULT_MAX_RESTARTS);
  }

  private Function<Throwable, Boolean> buildRetryFunction() {
    return ExceptionSupport::isConsumerStreamRetryable;
  }

  private Predicate<Long> stopRepeatingPredicate() {
    return attemptCount -> {

      if (streamConfiguration.streamLimit() != StreamConfiguration.DEFAULT_STREAM_TIMEOUT) {
        logger.debug(
            "op=repeater msg=will not continue to restart, request for a bounded number of events detected stream_limit={} restarts={}",
            streamConfiguration.streamLimit(), attemptCount);
        return true;
      }

      client.metricCollector().mark(MetricCollector.Meter.streamRestart);

      return false;
    };
  }

  private <T> StreamBatchRecord<T> lineToStreamBatchRecord(String line,
      TypeLiteral<T> typeLiteral, Response response, StreamConfiguration sc) {

    logger.debug("op=line_to_batch line={}, res={}", line, response);

    if (sc.isSubscriptionStream()) {
      String sessionId = response.headers().get(X_NAKADI_STREAM_ID).get(0);
      return jsonBatchSupport.lineToSubscriptionStreamBatchRecord(
          line, typeLiteral.type(), streamOffsetObserver(), sessionId, sc.subscriptionId());
    } else {
      return jsonBatchSupport.lineToEventStreamBatchRecord(
          line, typeLiteral.type(), streamOffsetObserver());
    }
  }

  private void setupRxErrorHandler() {
    RxJavaPlugins.setErrorHandler(
        t -> {
          if (t instanceof java.util.concurrent.RejectedExecutionException) {
             // can happen with a processor stop and another start if the old one is interrupted
            logger.warn("op=unhandled_rejected_execution action=continue {}", t.getMessage());
          } else {
            if (t instanceof NonRetryableNakadiException) {
              logger.error(String.format(
                  "op=unhandled_non_retryable_exception action=stopping type=NonRetryableNakadiException %s ",
                  ((NonRetryableNakadiException) t).problem()), t);

              stopStreaming();

            } else if (t instanceof Error) {
              logger.error(String.format(
                  "op=unhandled_error action=stopping type=NonRetryableNakadiException %s ",
                  t.getMessage()), t);

              stopStreaming();

            } else {
              logger.error(
                  String.format("unhandled_unknown_exception action=stopping type=%s %s",
                      t.getClass().getSimpleName(), t.getMessage()),
                  t);

              stopStreaming();
            }
          }
        }
    );
  }

  private void handleUncaught(Thread t, Throwable e, String name) {
    if (ExceptionSupport.isInterruptedIOException(e)) {
      Thread.currentThread().interrupt();
      logger.warn(String.format(
          "op=handle_exception action=interrupt_and_continue type=InterruptedIOException %s %s",
          name, t), e);
    } else {
      if (e instanceof NonRetryableNakadiException) {
        logger.error(String.format(
            "op=handle_exception action=stopping type=NonRetryableNakadiException %s %s %s", name,
            t, ((NonRetryableNakadiException) e).problem()), e);
      } else {
        logger.error(String.format("op=handle_exception action=stopping type=%s %s %s",
            e.getClass().getSimpleName(), name, t), e);
      }

      failedProcessorException = e;
      stopStreaming();
    }
  }

  @VisibleForTesting
  StreamOffsetObserver streamOffsetObserver() {
    return streamOffsetObserver;
  }

  @VisibleForTesting
  String findEventTypeNameForSubscription(StreamConfiguration sc) {
    nakadi.Subscription sub = client.resources().subscriptions().find(sc.subscriptionId());
    return sub.eventTypes().get(0);
  }

  @SuppressWarnings("WeakerAccess")
  public static class Builder {

    private NakadiClient client;
    private StreamObserverProvider streamObserverProvider;
    private SubscriptionOffsetCheckpointer checkpointer;
    private StreamOffsetObserver streamOffsetObserver;
    private StreamConfiguration streamConfiguration;
    private StreamProcessorRequestFactory streamProcessorRequestFactory;

    public Builder() {
    }

    public StreamProcessor build() {
      NakadiException.throwNonNull(streamConfiguration, "Please provide a stream configuration");

      if (streamConfiguration.isSubscriptionStream() && streamConfiguration.isEventTypeStream()) {
        throw new NakadiException(Problem.localProblem(
            "Cannot be configured with both a subscriptionId and an eventTypeName",
            String.format("subscriptionId=%s eventTypeName=%s",
                streamConfiguration.subscriptionId(), streamConfiguration.eventTypeName())));
      }

      if (!streamConfiguration.isSubscriptionStream() && !streamConfiguration.isEventTypeStream()) {
        throw new NakadiException(
            Problem.localProblem("Please supply either a subscription id or an event type", ""));
      }

      NakadiException.throwNonNull(client, "Please provide a client");
      NakadiException.throwNonNull(streamObserverProvider,
          "Please provide a StreamObserverProvider");

      if (streamConfiguration.isSubscriptionStream() && streamOffsetObserver == null) {
        if (checkpointer == null) {
          this.checkpointer =
              new SubscriptionOffsetCheckpointer(client).suppressInvalidSessionException(false);
        }

        this.streamOffsetObserver = new SubscriptionOffsetObserver(checkpointer);
      }

      if (streamConfiguration.isEventTypeStream() && streamOffsetObserver == null) {
        this.streamOffsetObserver = new LoggingStreamOffsetObserver();
      }

      if (streamProcessorRequestFactory == null) {
        streamProcessorRequestFactory = new StreamProcessorRequestFactory(client);
      }

      return new StreamProcessor(this);
    }

    public Builder client(NakadiClient client) {
      this.client = client;
      return this;
    }

    /**
     * Deprecated since 0.9.7 and will be removed in 0.10.0. Scopes set here are ignored.
     *
     * @return this
     */
    @SuppressWarnings("unused") @Deprecated
    public Builder scope(String scope) {
      return this;
    }

    public Builder streamObserverFactory(
        StreamObserverProvider streamObserverProvider) {
      this.streamObserverProvider = streamObserverProvider;
      return this;
    }

    public Builder streamOffsetObserver(StreamOffsetObserver streamOffsetObserver) {
      this.streamOffsetObserver = streamOffsetObserver;
      return this;
    }

    public Builder streamConfiguration(StreamConfiguration streamConfiguration) {
      this.streamConfiguration = streamConfiguration;
      return this;
    }

    @Unstable
    public Builder checkpointer(SubscriptionOffsetCheckpointer checkpointer) {
      this.checkpointer = checkpointer;
      return this;
    }

    @VisibleForTesting
    Builder streamProcessorRequestFactory(StreamProcessorRequestFactory factory) {
      this.streamProcessorRequestFactory = factory;
      return this;
    }
  }
}
