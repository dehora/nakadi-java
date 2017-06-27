package nakadi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Scheduler;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.exceptions.UndeliverableException;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API support for streaming events to a consumer.
 * <p>
 * Supports both the name event type streams and the more recent subscription based streams.
 * The API's connection streaming models are fundamentally the same, but have some differences
 * in  detail, such as request parameters and the structure of the batch cursor. Users
 * should consult the Nakadi API documentation and {@link StreamConfiguration} for details
 * on the streaming options.
 * </p>
 *
 * @see nakadi.StreamConfiguration
 */
public class StreamProcessor implements StreamProcessorManaged {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final int DEFAULT_HALF_OPEN_CONNECTION_GRACE_SECONDS = 90;
  private static final int DEFAULT_BUFFER_SIZE = 16000;
  private final NakadiClient client;
  private final StreamConfiguration streamConfiguration;
  private final StreamObserverProvider streamObserverProvider;
  private final StreamOffsetObserver streamOffsetObserver;
  private final ExecutorService streamProcessorExecutorService;
  private final JsonBatchSupport jsonBatchSupport;
  private final long maxRetryDelay;
  private final String scope;
  // non builder supplied
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final ExecutorService monoIoExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setNameFormat("nakadi-java-io-%d")
          .setUncaughtExceptionHandler((t, e) -> handleUncaught(t, e, "stream_processor_err_io"))
          .build());
  private final ExecutorService monoComputeExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setNameFormat("nakadi-java-compute-%d")
          .setUncaughtExceptionHandler(
              (t, e) -> handleUncaught(t, e, "stream_processor_err_compute"))
          .build());

  private final Scheduler monoIoScheduler = Schedulers.from(monoIoExecutor);
  private final Scheduler monoComputeScheduler = Schedulers.from(monoComputeExecutor);
  private final CountDownLatch startLatch;
  private final StreamProcessorRequestFactory streamProcessorRequestFactory;
  private CompositeDisposable composite;

  @VisibleForTesting
  @SuppressWarnings("unused") StreamProcessor(NakadiClient client,
      StreamProcessorRequestFactory streamProcessorRequestFactory) {
    this.client = client;
    this.streamConfiguration = null;
    this.streamObserverProvider = null;
    this.streamOffsetObserver = null;
    this.streamProcessorExecutorService = null;
    this.jsonBatchSupport = new JsonBatchSupport(client.jsonSupport());
    this.maxRetryDelay = StreamConnectionRetryFlowable.DEFAULT_MAX_DELAY_SECONDS;
    this.scope = null;
    this.composite = new CompositeDisposable();
    startLatch = new CountDownLatch(1);
    this.streamProcessorRequestFactory = streamProcessorRequestFactory;
  }

  private StreamProcessor(Builder builder) {
    this.streamConfiguration = builder.streamConfiguration;
    this.client = builder.client;
    this.streamObserverProvider = builder.streamObserverProvider;
    this.streamOffsetObserver = builder.streamOffsetObserver;
    this.streamProcessorExecutorService = builder.executorService;
    this.jsonBatchSupport = new JsonBatchSupport(client.jsonSupport());
    this.maxRetryDelay = streamConfiguration.maxRetryDelaySeconds();
    this.scope = builder.scope;
    this.composite = new CompositeDisposable();
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

  private static boolean isInterrupt(Throwable e) {
    // unwrap to see if this is an InterruptedIOException bubbled up from rx/okio
    if (e instanceof UndeliverableException) {
      if (e.getCause() != null && e.getCause() instanceof UncheckedIOException) {
        if (e.getCause().getCause() != null &&
            e.getCause().getCause() instanceof InterruptedIOException) {
          return true;
        }
      }
    }
    return false;
  }

  private static ExecutorService newStreamProcessorExecutorService() {
    final ThreadFactory tf =
        new ThreadFactoryBuilder()
            .setUncaughtExceptionHandler((t, e) -> handleUncaught(t, e, "stream_processor_err"))
            .setNameFormat("nakadi-java").build();
    return Executors.newFixedThreadPool(1, tf);
  }

  private static void handleUncaught(Thread t, Throwable e, String name) {
    if (isInterrupt(e)) {
      Thread.currentThread().interrupt();
    } else {
      logger.error("{} {}, {}", name, t, e);
    }
  }

  /**
   * Start consuming the stream. This runs in a background executor and will not block the
   * calling thread. Callers must hold onto a reference in order to be able to shut it down.
   *
   * <p>
   * Calling start multiple times is the same as calling it once, when stop is not also called
   * or interleaved with.
   * </p>
   *
   * @throws IllegalStateException if the processor has already been stopped.
   * @see #stop()
   */
  public void start() throws IllegalStateException {
    if(stopped.get()) {
      throw new IllegalStateException("processor has already been stopped and cannot be restarted");
    }

    if (!started.getAndSet(true)) {
      executorService().submit(this::startStreaming);
    }

    waitingOnStart();
  }

  /**
   * Perform a controlled shutdown of the stream. The {@link StreamObserver} will have
   * its onCompleted or onError called under normal circumstances. The {@link StreamObserver}
   * in turn can call its {@link StreamOffsetObserver} to perform cleanup.
   *
   * <p>
   * Calling stop multiple times is the same as calling it once, when start is not also called
   * or interleaved with.
   * </p>
   *
   * @see #start()
   */
  public void stop() {

    waitingOnStart();

    if (started.getAndSet(false)) {
      stopStreaming();
    }

    stopped.getAndSet(true);
  }

  void startStreaming() {
    stream(streamConfiguration, streamObserverProvider);
    startLatch.countDown();
  }

  void stopStreaming() {

    logger.info("stopping subscriber");
    composite.dispose();

    /*
    call through the rxjava scheduler contract
    */
    logger.info("stopping_scheduler name=monoIoScheduler {}", monoIoScheduler);
    monoIoScheduler.shutdown();

    logger.info("stopping_scheduler name=monoComputeScheduler {}", monoIoScheduler);
    monoComputeScheduler.shutdown();

    logger.info("stopping_scheduler name=all_schedulers");
    Schedulers.shutdown();

    logger.info("stopping_executor name=stream_processor");
    ExecutorServiceSupport.shutdown(streamProcessorExecutorService);

    /*
    stop these underlying executors directly. these are wrapped as rxjava schedulers, but calling
    shutdown via the wrapping Scheduler or Schedulers.shutdown doesn't kill their threads
     */
    logger.info("stopping_executor name=monoIoScheduler");
    ExecutorServiceSupport.shutdown(monoIoExecutor);

    logger.info("stopping_executor name=monoComputeScheduler");
    ExecutorServiceSupport.shutdown(monoComputeExecutor);
  }

  @VisibleForTesting
  StreamOffsetObserver streamOffsetObserver() {
    return streamOffsetObserver;
  }

  private ExecutorService executorService() {
    return streamProcessorExecutorService;
  }

  private void waitingOnStart() {
    try {
      startLatch.await(60, TimeUnit.SECONDS);
      logger.info("stream_processor has started");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private <T> void stream(StreamConfiguration sc,
      StreamObserverProvider<T> observerProvider) {

    final StreamObserver<T> observer = observerProvider.createStreamObserver();
    final TypeLiteral<T> typeLiteral = observerProvider.typeLiteral();
    final Flowable<StreamBatchRecord<T>> observable =
        this.buildStreamObservable(observer, sc, typeLiteral);

    /*
     Do processing on monoComputeScheduler; if the monoIoScheduler (or any shared
     single thread executor) is used, the pipeline can lock up as the thread is dominated by
     io and never frees to process batches. monoComputeScheduler is a single thread executor
     to make things easier to reason about for now wrt to ordering/sequential batch processing
     (but the regular computation scheduler could work as well maybe).
    */

    Optional<Integer> maybeBuffering = observer.requestBuffer();
    if (maybeBuffering.isPresent()) {
      logger.info("Creating buffering subscriber buffer={} {}", maybeBuffering.get(), sc);
      /*
      if the stream observer wants buffering set that up; it will still see
      discrete batches but the rx observer wrapping around it here will be given
      buffered up lists
     */
      composite.add(observable.observeOn(monoComputeScheduler)
          .buffer(maybeBuffering.get())
          .subscribeWith(
              new StreamBatchRecordBufferingSubscriber<>(observer, client.metricCollector())));
    } else {
      logger.info("Creating regular subscriber {}", sc);

      composite.add(observable.observeOn(monoComputeScheduler)
          .subscribeWith(
              new StreamBatchRecordSubscriber<>(observer, client.metricCollector())));
    }
  }

  private <T> Flowable<StreamBatchRecord<T>> buildStreamObservable(
      StreamObserver<T> streamObserver,
      StreamConfiguration streamConfiguration,
      TypeLiteral<T> typeLiteral) {

    /*
     compute a timeout after which we assume the server's gone away or we're on
     one end of a half-open connection. this is a big downside trying to emulate
     a streaming model over http get; you really need bidirectional comms instead
     todo: make these configurable
     */
    TimeUnit halfOpenUnit = TimeUnit.SECONDS;
    long halfOpenGrace = DEFAULT_HALF_OPEN_CONNECTION_GRACE_SECONDS;
    long batchFlushTimeoutSeconds = this.streamConfiguration.batchFlushTimeoutSeconds();
    long halfOpenKick = halfOpenUnit.toSeconds(batchFlushTimeoutSeconds + halfOpenGrace);
    logger.info(
        "configuring half open timeout, batch_flush_timeout={}, grace_period={}, disconnect_after={} {}",
        batchFlushTimeoutSeconds, halfOpenGrace, halfOpenKick, halfOpenUnit.name().toLowerCase());

    final Flowable<StreamBatchRecord<T>> flowable = Flowable.using(
        resourceFactory(streamConfiguration),
        observableFactory(typeLiteral, streamConfiguration),
        observableDispose()
    )
        /*
        monoIoScheduler: okhttp needs to be closed on the same thread that opened; using a
        single thread scheduler allows that to happen whereas the default/io/compute schedulers
        all use multiple threads which can cause resource leaks: http://bit.ly/2fe4UZH
        */
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
        /*
         todo: investigate why Integer.max causes
        io.reactivex.exceptions.UndeliverableException: java.lang.NegativeArraySizeException
         at io.reactivex.plugins.RxJavaPlugins.onError(RxJavaPlugins.java:366)
         */
        .onBackpressureBuffer(DEFAULT_BUFFER_SIZE, true, true);

    return Flowable.defer(() -> flowable);
  }

  private StreamConnectionRetryFlowable buildStreamConnectionRetryFlowable(
  ) {
    return new StreamConnectionRetryFlowable(ExponentialRetry.newBuilder()
        .initialInterval(StreamConnectionRetryFlowable.DEFAULT_INITIAL_DELAY_SECONDS,
            StreamConnectionRetryFlowable.DEFAULT_TIME_UNIT)
        .maxInterval(maxRetryDelay, StreamConnectionRetryFlowable.DEFAULT_TIME_UNIT)
        .build(),
        buildRetryFunction(), client.metricCollector());
  }

  private <T> FlowableTransformer<StreamBatchRecord<T>, StreamBatchRecord<T>> buildRestartHandler() {
    long restartDelay = StreamConnectionRestart.DEFAULT_DELAY_SECONDS;
    TimeUnit restartDelayUnit = StreamConnectionRestart.DEFAULT_DELAY_UNIT;
    int maxRestarts = StreamConnectionRestart.DEFAULT_MAX_RESTARTS;
    return new StreamConnectionRestart()
        .repeatWhenWithDelayAndUntil(
            stopRepeatingPredicate(), restartDelay, restartDelayUnit, maxRestarts);
  }

  private Function<Throwable, Boolean> buildRetryFunction() {
    return ExceptionSupport::isConsumerStreamRetryable;
  }

  private Predicate<Long> stopRepeatingPredicate() {
    return attemptCount -> {

      // todo: track the actual events checkpointed or seen instead
      if (streamConfiguration.streamLimit() != StreamConfiguration.DEFAULT_STREAM_TIMEOUT) {
        logger.info(
            "stream repeater will not continue to restart, request for a bounded number of events detected stream_limit={} restarts={}",
            streamConfiguration.streamLimit(), attemptCount);
        return true;
      }

      client.metricCollector().mark(MetricCollector.Meter.streamRestart);

      return false;
    };
  }

  private Consumer<? super Response> observableDispose() {
    return (response) -> {
      logger.info("stream_connection_dispose thread {} {} {}", Thread.currentThread().getName(),
          response.hashCode(), response);
      try {
        response.responseBody().close();
        logger.info("stream_connection_dispose_ok thread {} {} {}",
            Thread.currentThread().getName(), response.hashCode(), response);
      } catch (IOException e) {
        throw new NakadiException(
            Problem.networkProblem("failed to close stream response", e.getMessage()), e);
      }
    };
  }

  private <T> Function<? super Response, Flowable<StreamBatchRecord<T>>> observableFactory(
      TypeLiteral<T> typeLiteral, StreamConfiguration sc) {
    return (Response response) -> {
      final BufferedReader br = new BufferedReader(response.responseBody().asReader());
      return Flowable.fromIterable(br.lines()::iterator)
          .doOnError(throwable -> {

            boolean closed = false;
            final String tName = Thread.currentThread().getName();

            try {
              logger.info("stream_iterator_response_close_ask thread={} error={} {} {}",
                  tName, throwable.getMessage(), response.hashCode(), response);
              response.responseBody().close();
              closed = true;
              logger.info("stream_iterator_response_close_ok thread={} error={} {} {}",
                  tName, throwable.getMessage(), response.hashCode(), response);
            } catch (Exception e) {
              logger.warn(
                  "stream_iterator_response_close_error problem closing thread={} {} {} {} {}",
                  tName, e.getClass().getName(), e.getMessage(), response.hashCode(), response);
            } finally {
              if (!closed) {
                try {
                  response.responseBody().close();
                  closed = true;
                } catch (IOException e) {
                  logger.warn(
                      "stream_iterator_response_close_error  problem re-attempting close thread={} {} {} {} {}",
                      tName, e.getClass().getName(), e.getMessage(), response.hashCode(), response);
                }
              }

              if (!closed) {
                logger.warn(
                    String.format(
                        "stream_iterator_response_close_failed did not close response thread=%s err=%s %s %s",
                        tName, throwable.getMessage(),
                        response.hashCode(), response), throwable);
              }
            }
          })
          .onBackpressureBuffer(DEFAULT_BUFFER_SIZE, true, true)
          .map(r -> lineToStreamBatchRecord(r, typeLiteral, response, sc))
          ;
    };
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Callable<Response> resourceFactory(StreamConfiguration sc) {
    return streamProcessorRequestFactory.createCallable(sc);
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Response requestStreamConnection(String url, ResourceOptions options, Resource resource) {
    return resource.requestThrowing(Resource.GET, url, options);
  }

  private <T> StreamBatchRecord<T> lineToStreamBatchRecord(String line,
      TypeLiteral<T> typeLiteral, Response response, StreamConfiguration sc) {

    logger.debug("tokenized line from stream {}, {}", line, response);

    if (sc.isSubscriptionStream()) {
      String xNakadiStreamId = response.headers().get("X-Nakadi-StreamId").get(0);
      return jsonBatchSupport.lineToSubscriptionStreamBatchRecord(
          line, typeLiteral.type(), streamOffsetObserver(), xNakadiStreamId, sc.subscriptionId());
    } else {
      return jsonBatchSupport.lineToEventStreamBatchRecord(
          line, typeLiteral.type(), streamOffsetObserver());
    }
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Resource buildResource(StreamConfiguration sc) {
    return client.resourceProvider()
        .newResource()
        .readTimeout(sc.readTimeoutMillis(), TimeUnit.MILLISECONDS)
        .connectTimeout(sc.connectTimeoutMillis(), TimeUnit.MILLISECONDS);
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
    private ExecutorService executorService;
    private String scope;
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

      if (executorService == null) {
        this.executorService = newStreamProcessorExecutorService();
      }

      this.scope =
          Optional.ofNullable(scope).orElse(TokenProvider.NAKADI_EVENT_STREAM_READ);

      if (streamProcessorRequestFactory == null) {
        streamProcessorRequestFactory = new StreamProcessorRequestFactory(client, scope);
      }

      return new StreamProcessor(this);
    }

    public Builder client(NakadiClient client) {
      this.client = client;
      return this;
    }

    public Builder scope(String scope) {
      this.scope = scope;
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

    @SuppressWarnings("unused")
    public Builder executorService(ExecutorService executorService) {
      this.executorService = executorService;
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
