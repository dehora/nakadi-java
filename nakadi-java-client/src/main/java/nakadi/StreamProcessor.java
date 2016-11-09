package nakadi;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

public class StreamProcessor implements StreamProcessorManaged {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final int DEFAULT_HALF_OPEN_CONNECTION_GRACE_SECONDS = 90;

  private final NakadiClient client;
  private final StreamConfiguration streamConfiguration;
  private final StreamObserverProvider streamObserverProvider;
  private final StreamOffsetObserver streamOffsetObserver;
  private final ExecutorService executorService;
  private final JsonBatchSupport jsonBatchSupport;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Scheduler monoIoScheduler;
  private final Scheduler monoComputeScheduler;
  private final long maxRetryDelay;
  private final int maxRetryAttempts;

  // non builder supplied
  private Subscription subscription;

  // todo: give these an orderly shutdown via ExecutorServiceSupport
  private final ExecutorService monoIoExecutor;
  private final ExecutorService monoComputeExecutor;

  @VisibleForTesting
  @SuppressWarnings("unused") StreamProcessor(NakadiClient client) {
    this.client = client;
    this.streamConfiguration = null;
    this.streamObserverProvider = null;
    this.streamOffsetObserver = null;
    this.executorService = null;
    this.jsonBatchSupport = new JsonBatchSupport(client.jsonSupport());
    this.monoIoExecutor = Executors.newSingleThreadExecutor();
    this.monoComputeExecutor = Executors.newSingleThreadExecutor();
    this.monoIoScheduler = Schedulers.from(monoIoExecutor);
    this.monoComputeScheduler = Schedulers.from(monoComputeExecutor);
    this.maxRetryDelay = StreamConnectionRetry.DEFAULT_MAX_DELAY_SECONDS;
    this.maxRetryAttempts = StreamConnectionRetry.DEFAULT_MAX_ATTEMPTS;
  }

  private StreamProcessor(Builder builder) {
    this.streamConfiguration = builder.streamConfiguration;
    this.client = builder.client;
    this.streamObserverProvider = builder.streamObserverProvider;
    this.streamOffsetObserver = builder.streamOffsetObserver;
    this.executorService = builder.executorService;
    this.jsonBatchSupport = new JsonBatchSupport(client.jsonSupport());
    this.monoIoExecutor = Executors.newSingleThreadExecutor();
    this.monoComputeExecutor = Executors.newSingleThreadExecutor();
    this.monoIoScheduler = Schedulers.from(monoIoExecutor);
    this.monoComputeScheduler = Schedulers.from(monoComputeExecutor);
    this.maxRetryDelay = streamConfiguration.maxRetryDelaySeconds();
    this.maxRetryAttempts = streamConfiguration.maxRetryAttempts();
  }

  public static StreamProcessor.Builder newBuilder(NakadiClient client) {
    return new StreamProcessor.Builder().client(client);
  }

  public void start() {
    if (!started.getAndSet(true)) {
      executorService().submit(this::startStreaming);
    }
  }

  public void stop() {
    if (started.getAndSet(false)) {
      subscription.unsubscribe();
      ExecutorServiceSupport.shutdown(executorService());
    }
  }

  @VisibleForTesting
  StreamOffsetObserver streamOffsetObserver() {
    return streamOffsetObserver;
  }

  private ExecutorService executorService() {
    return executorService;
  }

  private void startStreaming() {
    stream(streamConfiguration, streamObserverProvider, streamOffsetObserver);
  }

  private <T> void stream(StreamConfiguration sc,
      StreamObserverProvider<T> observerProvider,
      StreamOffsetObserver offsetObserver) {

    StreamObserver<T> observer = observerProvider.createStreamObserver();
    TypeLiteral<T> typeLiteral = observerProvider.typeLiteral();
    Observable<StreamBatchRecord<T>> observable =
        this.buildStreamObservable(observer, sc, offsetObserver, typeLiteral);

    /*
      if the stream observer wants buffering set that up; it will still see
      discrete batches but the rx observer wrapping around it here will be given
      buffered up lists
     */

    /*
    Do processing on monoComputeScheduler; if these also use monoIoScheduler (or any shared
    single thread executor), the pipeline can lock up as the thread is dominated by io and
    never frees to process batches. monoComputeScheduler is a single thread executor to make
    things easier to reason about for now wrt to ordering/sequential batch processing (but the
    regular computation scheduler could work as well maybe).
      */

    Optional<Integer> maybeBuffering = observer.requestBuffer();
    if (maybeBuffering.isPresent()) {
      logger.info("Creating buffering subscriber buffer={} {}", maybeBuffering.get(), sc);
      subscription = observable
          .observeOn(monoComputeScheduler)
          .buffer(maybeBuffering.get())
          .subscribe(
              new StreamBatchRecordBufferingSubscriber<>(observer, client.metricCollector()));
    } else {
      logger.info("Creating regular subscriber {}", sc);
      subscription = observable
          .observeOn(monoComputeScheduler)
          .subscribe(new StreamBatchRecordSubscriber<>(
              observer, client.metricCollector()));
    }
  }

  private <T> Observable<StreamBatchRecord<T>> buildStreamObservable(
      StreamObserver<T> streamObserver,
      StreamConfiguration streamConfiguration,
      StreamOffsetObserver streamOffsetObserver,
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

    /*
    monoIoScheduler: okhttp needs to be closed on the same thread that opened; using a
    single thread scheduler allows that to happen whereas the default/io/compute schedulers
    all use multiple threads which can cause resource leaks: http://bit.ly/2fe4UZH
    */

    return
        Observable.defer(() ->
            Observable.using(
                resourceFactory(streamConfiguration),
                observableFactory(streamOffsetObserver, typeLiteral, streamConfiguration),
                observableDispose()
            )
                .subscribeOn(monoIoScheduler)
                .unsubscribeOn(monoIoScheduler)
                .doOnSubscribe(streamObserver::onStart)
                .doOnUnsubscribe(streamObserver::onStop)
                .timeout(halfOpenKick, halfOpenUnit)
                .compose(buildRetryHandler(streamConfiguration))
                .compose(buildRestartHandler()
                )
        );
  }

  private <T> Observable.Transformer<StreamBatchRecord<T>, StreamBatchRecord<T>> buildRetryHandler(
      StreamConfiguration streamConfiguration) {
    int initialDelay = StreamConnectionRetry.DEFAULT_INITIAL_DELAY_SECONDS;
    TimeUnit unit = StreamConnectionRetry.DEFAULT_TIME_UNIT;
    final Func1<Throwable, Boolean> isRetryable = buildRetryFunction(streamConfiguration);
    return new StreamConnectionRetry()
        .retryWhenWithBackoff(
            maxRetryAttempts, initialDelay, maxRetryDelay, unit, monoIoScheduler, isRetryable);
  }

  private <T> Observable.Transformer<StreamBatchRecord<T>, StreamBatchRecord<T>> buildRestartHandler() {
    long restartDelay = StreamConnectionRestart.DEFAULT_DELAY_SECONDS;
    TimeUnit restartDelayUnit = StreamConnectionRestart.DEFAULT_DELAY_UNIT;
    int maxRestarts = StreamConnectionRestart.DEFAULT_MAX_RESTARTS;
    return new StreamConnectionRestart()
        .repeatWhenWithDelayAndUntil(
            stopRepeatingPredicate(), restartDelay, restartDelayUnit, maxRestarts);
  }

  private Func1<Throwable, Boolean> buildRetryFunction(StreamConfiguration sc) {
    final Func1<Throwable, Boolean> isRetryable;
    if (sc.isSubscriptionStream()) {
      isRetryable = StreamExceptionSupport::isSubscriptionRetryable;
    } else {
      isRetryable = StreamExceptionSupport::isRetryable;
    }
    return isRetryable;
  }

  private Func1<Long, Boolean> stopRepeatingPredicate() {
    return new Func1<Long, Boolean>() {
      @Override public Boolean call(Long attemptCount) {

        // todo: track the actual events checkpointed or seen instead
        if (streamConfiguration.streamLimit() != StreamConfiguration.DEFAULT_STREAM_TIMEOUT) {
          logger.info(
              "stream repeater will not continue to restart, request for a bounded number of events detected stream_limit={} restarts={}",
              streamConfiguration.streamLimit(), attemptCount);
          return true;
        }
        return false;
      }
    };
  }

  private Action1<? super Response> observableDispose() {
    return (response) -> {
      logger.info("disposing connection {} {}", response.hashCode(), response);
      try {
        response.responseBody().close();
      } catch (IOException e) {
        throw new NakadiException(
            Problem.networkProblem("failed to close stream response", e.getMessage()), e);
      }
    };
  }

  private <T> Func1<? super Response, Observable<StreamBatchRecord<T>>> observableFactory(
      StreamOffsetObserver streamOffsetObserver, TypeLiteral<T> typeLiteral,
      StreamConfiguration sc) {
    return (response) -> {

      final List<T> emptyList = Lists.newArrayList();
      final Observable<StreamBatchRecord<T>> forEmpty =
          response.statusCode() != 200 ?
              Observable.just(emptyBatch(streamOffsetObserver, emptyList))
              : Observable.empty();

      final BufferedReader br = new BufferedReader(response.responseBody().asReader());
      return Observable.from(br.lines()::iterator)
          .map(r -> lineToStreamBatchRecord(r, typeLiteral, response, sc))
          .switchIfEmpty(forEmpty);
    };
  }

  private Func0<Response> resourceFactory(StreamConfiguration sc) {
    return () -> {
      String eventTypeName = resolveEventTypeName(sc);
      String url = StreamResourceSupport.buildStreamUrl(client.baseURI(), sc);
      ResourceOptions options = StreamResourceSupport.buildResourceOptions(client, sc);
      logger.info("resourceFactory mode={} resolved_event_name={} url={}",
          sc.isEventTypeStream() ? "eventStream" : "subscriptionStream", eventTypeName, url);
      Resource resource = buildResource(sc);
      /*
       sometimes we can get a 409 from here (Conflict; No free slots) on the subscription; this
       can happen when we disconnect if we think there's a zombie connection and throw a timeout.
       the retry/restarts will handle it
      */
      Response response = resource.requestThrowing(Resource.GET, url, options);
      logger.info("opening connection {} {}", response.hashCode(), response);
      return response;
    };
  }

  private <T> StreamBatchRecordReal<T> emptyBatch(StreamOffsetObserver observer, List<T> list) {
    return new StreamBatchRecordReal<>(new EventStreamBatch<>(null, null, list), observer);
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

  private Resource buildResource(StreamConfiguration sc) {
    return client.resourceProvider()
        .newResource()
        .readTimeout(sc.readTimeoutMillis(), TimeUnit.MILLISECONDS)
        .connectTimeout(sc.connectTimeoutMillis(), TimeUnit.MILLISECONDS);
  }

  private String resolveEventTypeName(StreamConfiguration sc) {
    String eventTypeName;
    if (sc.isSubscriptionStream()) {
      nakadi.Subscription subscription =
          client.resources().subscriptions().find(sc.subscriptionId());
      eventTypeName = subscription.eventTypes().get(0);
    } else {
      eventTypeName = sc.eventTypeName();
    }
    return eventTypeName;
  }

  @SuppressWarnings("WeakerAccess")
  public static class Builder {

    private NakadiClient client;
    private StreamObserverProvider streamObserverProvider;
    private StreamOffsetObserver streamOffsetObserver;
    private StreamConfiguration streamConfiguration;
    private ExecutorService executorService;

    public Builder() {
    }

    public StreamProcessor build() {
      NakadiException.throwNonNull(streamConfiguration, "Please provide a stream configuration");

      if (streamConfiguration.isSubscriptionStream() && streamConfiguration.isEventTypeStream()) {
        throw new NakadiException(Problem.localProblem(
            "Cannot be configured with both a subscription id or an event type",
            String.format("subscriptionId=%s eventTypeName=%s",
                streamConfiguration.subscriptionId(), streamConfiguration.eventTypeName())));
      }

      if (!streamConfiguration.isSubscriptionStream() && !streamConfiguration.isEventTypeStream()) {
        throw new NakadiException(
            Problem.localProblem("Please supply either a subscription id or an event type", ""));
      }

      NakadiException.throwNonNull(client, "Please provide a client");
      NakadiException.throwNonNull(streamObserverProvider, "Please provide an observer factory");

      if (streamConfiguration.isSubscriptionStream() && streamOffsetObserver == null) {
        this.streamOffsetObserver = new SubscriptionOffsetObserver(client);
      }

      if (streamConfiguration.isEventTypeStream() && streamOffsetObserver == null) {
        this.streamOffsetObserver = new LoggingStreamOffsetObserver();
      }

      if (executorService == null) {
        this.executorService = ExecutorServiceSupport.newExecutorService();
      }

      return new StreamProcessor(this);
    }

    public Builder client(NakadiClient client) {
      this.client = client;
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
  }
}
