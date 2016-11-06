package nakadi;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
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

  private final NakadiClient client;
  private final StreamConfiguration streamConfiguration;
  private final StreamObserverProvider streamObserverProvider;
  private final StreamOffsetObserver streamOffsetObserver;
  private final ExecutorService executorService;
  private final JsonBatchSupport jsonBatchSupport;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Scheduler computeScheduler;
  private final Scheduler ioScheduler;
  private final long maxRetryDelay;
  private final int maxRetryAttempts;
  // non builder supplied
  private Subscription subscription;

  @VisibleForTesting
  @SuppressWarnings("unused") StreamProcessor(NakadiClient client) {
    this.client = client;
    this.streamConfiguration = null;
    this.streamObserverProvider = null;
    this.streamOffsetObserver = null;
    this.executorService = null;
    this.jsonBatchSupport = new JsonBatchSupport(client.jsonSupport());
    this.ioScheduler = Schedulers.io();
    this.computeScheduler = Schedulers.computation();
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
    this.ioScheduler = Schedulers.io();
    this.computeScheduler = Schedulers.computation();
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

    // todo: replace with per event tracking; this doesn't work if subs ever allow multiple types
    String eventTypeName = resolveEventTypeName(sc);

    StreamObserver<T> observer = observerProvider.createStreamObserver();
    TypeLiteral<T> typeLiteral = observerProvider.typeLiteral();
    Observable<StreamBatchRecord<T>> observable =
        this.createStreamBatchRecordObservable(observer, sc, offsetObserver, typeLiteral);

    /*
      if the stream observer wants buffering set that up; it will still see
      discrete batches but the rx observer wrapping around it here will be given
      buffered up lists
     */
    Optional<Integer> maybeBuffering = observer.requestBuffer();
    if (maybeBuffering.isPresent()) {
      logger.info("Creating buffering subscriber, buffer={}", maybeBuffering.get());
      subscription = observable
          .buffer(maybeBuffering.get())
          .subscribe(
              new StreamBatchRecordBufferingSubscriber<>(observer, client.metricCollector()));
    } else {
      logger.info("Creating regular subscriber");
      subscription = observable
          .observeOn(computeScheduler)
          .subscribe(new StreamBatchRecordSubscriber<>(
              observer, client.metricCollector()));
    }
  }

  private <T> Observable<StreamBatchRecord<T>> createStreamBatchRecordObservable(
      StreamObserver<T> observer,
      StreamConfiguration sc,
      StreamOffsetObserver streamOffsetObserver,
      TypeLiteral<T> typeLiteral) {

    Observable<StreamBatchRecord<T>> observable = Observable.using(
        resourceFactory(sc),
        observableFactory(streamOffsetObserver, typeLiteral, sc),
        observableDispose());

    int initialDelay = StreamConnectionRetry.DEFAULT_INITIAL_DELAY_SECONDS;
    TimeUnit unit = StreamConnectionRetry.DEFAULT_TIME_UNIT;

    // todo: make these configurable
    long restartDelay = StreamConnectionRestart.DEFAULT_DELAY_SECONDS;
    TimeUnit restartDelayUnit = StreamConnectionRestart.DEFAULT_DELAY_UNIT;
    int maxRestarts = StreamConnectionRestart.DEFAULT_MAX_RESTARTS;

    final Func1<Throwable, Boolean> isRetryable;
    if (sc.isSubscriptionStream()) {
      isRetryable = StreamExceptionSupport::isSubscriptionRetryable;
    } else {
      isRetryable = StreamExceptionSupport::isRetryable;
    }

    observable = observable
        .subscribeOn(ioScheduler)
        .unsubscribeOn(ioScheduler)
        .doOnSubscribe(observer::onStart)
        .doOnUnsubscribe(observer::onStop)
        .compose(
            new StreamConnectionRetry()
                .retryWhenWithBackoff(
                    maxRetryAttempts, initialDelay, maxRetryDelay, unit, ioScheduler, isRetryable)
        )
        .compose(
            new StreamConnectionRestart()
                .repeatWhenWithDelayAndUntil(
                    stopRepeatingPredicate(), restartDelay, restartDelayUnit, maxRestarts)
        )
    ;
    return observable;
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
      logger.info("dispose " + response.hashCode());
      try {
        response.responseBody().close();
      } catch (IOException e) {
        e.printStackTrace();
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
      String url = StreamResourceSupport.buildStreamUrl(client.baseURI(), sc);
      ResourceOptions options = StreamResourceSupport.buildResourceOptions(client, sc);
      logger.info(String.format("resourceFactory mode=%s url=%s",
          sc.isEventTypeStream() ? "eventStream" : "subscriptionStream", url));
      return buildResource(sc).requestThrowing(Resource.GET, url, options);
    };
  }

  private <T> StreamBatchRecordReal<T> emptyBatch(StreamOffsetObserver observer, List<T> list) {
    return new StreamBatchRecordReal<>(new EventStreamBatch<>(null, null, list), observer);
  }

  private <T> StreamBatchRecord<T> lineToStreamBatchRecord(String line,
      TypeLiteral<T> typeLiteral, Response response, StreamConfiguration sc) {

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
