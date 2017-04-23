package nakadi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.internal.schedulers.ExecutorScheduler;
import io.reactivex.schedulers.Schedulers;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static nakadi.MetricCollector.Meter.retrySkipFinished;

class OkHttpResource implements Resource {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private static final String HEADER_AUTHORIZATION = "Authorization";
  private static final String APPLICATION_JSON_CHARSET_UTF8 = "application/json; charset=utf8";

  private final OkHttpClient okHttpClient;
  private final JsonSupport jsonSupport;
  private final MetricCollector metricCollector;
  private long connectTimeout = 0;
  private long readTimeout = 0;
  private volatile boolean hasPerRequestConnectTimeout;
  private volatile boolean hasPerRequestReadTimeout;
  private volatile RetryPolicy retryPolicy;
  private volatile Response response;

  OkHttpResource(OkHttpClient okHttpClient, JsonSupport jsonSupport, MetricCollector collector) {
    this.okHttpClient = okHttpClient;
    this.jsonSupport = jsonSupport;
    this.metricCollector = collector;
  }

  public OkHttpResource connectTimeout(long timeout, TimeUnit unit) {
    this.connectTimeout = unit.toMillis(timeout);
    hasPerRequestConnectTimeout = true;
    return this;
  }

  public OkHttpResource readTimeout(long timeout, TimeUnit unit) {
    this.readTimeout = unit.toMillis(timeout);
    hasPerRequestReadTimeout = true;
    return this;
  }

  public OkHttpResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  @Override
  public <Req> Response request(String method, String url, ResourceOptions options, Req body)
      throws NakadiException {

    // nb: defer delays the just() until we call toBlocking(); lets us set things up.

    /*
    if we have a retry in place use a throwing call and return the last response captured.
     the rx retry mechanism is driven by exceptions but the caller is asking for a response here
     so we don't propagate the exception
     */

    if (retryPolicy != null) {
      if (retryPolicy.isFinished()) {
        logger.warn("Cowardly, refusing to apply retry policy that is already finished {}",
            retryPolicy);
        metricCollector.mark(retrySkipFinished);
      } else {
        try {
          Observable<Response> observable =
              Observable.defer(
                  () -> Observable.just(requestThrowingInner(method, url, options, body)));
          Response first = observable.compose(buildRetry(retryPolicy)).blockingFirst();
          response = null; // zero out any previous responses
          return first;
        } catch (HttpException e) {
          logger.error("request with retry failed, {}", e.getMessage(), e);
          return response;
        }
      }
    }

    Observable<Response> observable =
        Observable.defer(() -> Observable.just(requestInner(method, url, options, body)));
    return observable.blockingFirst();
  }

  @Override
  public Response request(String method, String url, ResourceOptions options)
      throws NakadiException {
    /*
    if we have a retry in place use a throwing call and return the last response captured.
     the rx retry mechanism is driven by exceptions but the caller is asking for a response here
     so we don't propagate the exception
     */

    if (retryPolicy != null) {
      if (retryPolicy.isFinished()) {
        logger.warn("Cowardly, refusing to apply retry policy that is already finished {}",
            retryPolicy);
        metricCollector.mark(retrySkipFinished);
      } else {
        try {
          Observable<Response> observable =
              Observable.defer(
                  () -> Observable.just(requestThrowingInner(method, url, options, null)));
          Response first = observable.compose(buildRetry(retryPolicy)).blockingFirst();
          response = null; // zero out any previous responses
          return first;
        } catch (HttpException e) {
          logger.error("request with retry failed, {}", e.getMessage(), e);
          return response;
        }
      }
    }

    Observable<Response> observable =
        Observable.defer(() -> Observable.just(requestInner(method, url, options, null)));
    return observable.blockingFirst();
  }

  @Override public Response requestThrowing(String method, String url, ResourceOptions options)
      throws NakadiException {

    Observable<Response> observable =
        Observable.defer(() -> Observable.just(requestThrowingInner(method, url, options)));

    return maybeComposeRetryPolicy(observable).blockingFirst();
  }

  @Override
  public <Req> Response requestThrowing(String method, String url, ResourceOptions options,
      Req body)
      throws NakadiException {

    Observable<Response> observable =
        Observable.defer(() -> Observable.just(requestThrowingInner(method, url, options, body)));

    return maybeComposeRetryPolicy(observable).blockingFirst();
  }

  @Override
  public <Req> Response postEventsThrowing(String url, ResourceOptions options, Req body)
      throws AuthorizationException, ClientException, ServerException,
      RateLimitException, NakadiException {
    Observable<Response> observable =
        Observable.defer(() -> Observable.just(
            throwPostEventsIfError(requestInner(POST, url, options, body))));

    return maybeComposeRetryPolicy(observable).blockingFirst();
  }

  @Override
  public <Res> Res requestThrowing(String method, String url, ResourceOptions options,
      Class<Res> res) throws NakadiException {

    Observable<Response> observable =
        Observable.defer(() -> Observable.just(requestThrowingInner(method, url, options)));

    Response response = maybeComposeRetryPolicy(observable).blockingFirst();
    return marshalResponse(response, res);
  }

  @Override
  public <Req, Res> Res requestThrowing(String method, String url, ResourceOptions options,
      Req body, Class<Res> res)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    Observable<Response> observable =
        Observable.defer(() -> Observable.just(requestThrowingInner(method, url, options, body)));

    Response response = maybeComposeRetryPolicy(observable).blockingFirst();
    return marshalResponse(response, res);
  }

  private <Req> Response requestThrowingInner(String method, String url, ResourceOptions options,
      Req body) {
    return throwIfError(requestInner(method, url, options, body));
  }

  private <Req> Response requestInner(String method, String url, ResourceOptions options,
      Req body) {
    return executeRequest(prepareBuilder(method, url, options, body));
  }

  private Response requestThrowingInner(String method, String url, ResourceOptions options) {
    return requestThrowingInner(method, url, options, null);
  }

  private Observable<Response> maybeComposeRetryPolicy(final Observable<Response> observable) {
    if (retryPolicy != null) { // no policy set by caller
      if(retryPolicy.isFinished()) {
        /*
        this can happen if
        a) the policy is a no-op policy always returning finished
        b) it's being reused across requests, likely a client bug
         */
        logger.warn("Cowardly, refusing to apply retry policy that is already finished {}", retryPolicy);
        metricCollector.mark(retrySkipFinished);
      } else {
        return observable.compose(buildRetry(retryPolicy));
      }
    }
    return observable;
  }

  private <Req> Request.Builder prepareBuilder(String method, String url, ResourceOptions options,
      Req body) {
    Request.Builder builder;
    if (body != null) {
      if(body instanceof EventContentSupplier) {
        EventContentSupplier supplier = (EventContentSupplier)body;
        RequestBody requestBody =
            RequestBody.create(MediaType.parse(APPLICATION_JSON_CHARSET_UTF8), supplier.content());
        builder = new Request.Builder().url(url).method(method, requestBody);

      } else {
        String content = jsonSupport.toJson(body);
        RequestBody requestBody =
            RequestBody.create(MediaType.parse(APPLICATION_JSON_CHARSET_UTF8), content);
        builder = new Request.Builder().url(url).method(method, requestBody);
      }
    } else {
      builder = applyMethodForNoBody(method, url, new Request.Builder().url(url));
    }
    options.headers()
        .entrySet()
        .stream()
        /*
         okhttp deals with this automatically by setting Accept-Encoding: gzip as
         a default and setting it requires manual decompression
          */
        .filter(e -> !"Accept-Encoding".equalsIgnoreCase(e.getKey()) || !"gzip".equalsIgnoreCase(
            e.getValue().toString()))
        .forEach(e -> builder.addHeader(e.getKey(), e.getValue().toString()));

    applyAuthHeaderIfPresent(options, builder);
    return builder;
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Response executeRequest(Request.Builder builder) {
    try {
      return new OkHttpResponse(okHttpCall(builder));
    } catch (IOException e) {
      throw new NetworkException(Problem.networkProblem(e.getMessage(), ""), e);
    }
  }

  private void applyAuthHeaderIfPresent(ResourceOptions options, Request.Builder builder) {
    options.supplyToken().ifPresent(t -> builder.header(HEADER_AUTHORIZATION, t));
  }

  private ObservableTransformer<Response, Response> buildRetry(RetryPolicy backoff) {
    return new StreamConnectionRetry()
        .retryWhenWithBackoffObserver(backoff,
            Schedulers.computation(),
            ExceptionSupport::isEventStreamRetryable);
  }

  private okhttp3.Response okHttpCall(Request.Builder builder) throws IOException {

    if (hasPerRequestReadTimeout || hasPerRequestConnectTimeout) {

      final OkHttpClient.Builder clientBuilder = okHttpClient.newBuilder();
      if (hasPerRequestReadTimeout) {
        clientBuilder.readTimeout(readTimeout, TimeUnit.MILLISECONDS);
      }

      if (hasPerRequestConnectTimeout) {
        clientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
      }
      return clientBuilder.build().newCall(builder.build()).execute();
    } else {
      return okHttpClient.newCall(builder.build()).execute();
    }
  }

  private Request.Builder applyMethodForNoBody(String method, String url, Request.Builder builder) {
    // assume we're not dealing with put/post/patch here as there's no body
    if (Resource.DELETE.equals(method)) {
      return builder.delete();
    } else if (Resource.GET.equals(method)) {
      return builder.get();
    } else if (Resource.HEAD.equals(method)) {
      return builder.head();
    } else {
      logger.warn("unexpected_method_request_with_no_body method={} url={}", method, url);
      return builder.method(method, RequestBody.create(MediaType.parse("text/plain"), ""));
    }
  }

  private <Res> Res marshalResponse(Response response, Class<Res> res) {
    if (res != null && res.isAssignableFrom(Response.class)) {
      //noinspection unchecked  // safe cast
      return (Res) response;
    } else {
      return jsonSupport.fromJson(response.responseBody().asString(), res);
    }
  }

  private Response throwIfError(Response response) {
    int code = response.statusCode();
    if (code >= 200 && code < 300) {
      return response;
    } else {
      /*
       field the response as this allows us to return it when a non-throwing request
        with a retry fails (we have to wrap non-throwing requests as throwing to
        drive the rx retry mechanism)
        */
      this.response = response;
      return handleError(response);
    }
  }

  private Response throwPostEventsIfError(Response response) {
    int code = response.statusCode();
    if (code == 207 || code == 422) {
      return response;
    }
    return throwIfError(response);
  }

  private <T> T handleError(Response response) {
    String raw = response.responseBody().asString();
    Problem problem = Optional.ofNullable(jsonSupport.fromJson(raw, Problem.class))
        .orElse(Problem.noProblemo("no problem sent back from server", "", response.statusCode()));

    return throwProblem(response.statusCode(), problem);
  }

  private <T> T throwProblem(int code, Problem problem) {
    if (code == 403 || code == 401) {
      metricCollector.mark(MetricCollector.Meter.http401);
      throw new AuthorizationException(problem);
    } else if (code == 404 || code == 410) {
      metricCollector.mark(MetricCollector.Meter.http404);
      throw new NotFoundException(problem);
    } else if (code == 409) {
      metricCollector.mark(MetricCollector.Meter.http409);
      throw new ConflictException(problem);
    } else if (code == 412) {
      metricCollector.mark(MetricCollector.Meter.http412);
      // eg bad cursors: Precondition Failed; offset 98 for partition 0 is unavailable (412)
      throw new PreconditionFailedException(problem);
    } else if (code == 422) {
      metricCollector.mark(MetricCollector.Meter.http422);
      throw new InvalidException(problem);
    } else if (code == 429) {
      metricCollector.mark(MetricCollector.Meter.http429);
      throw new RateLimitException(problem);
    } else if (code == 400) {
      metricCollector.mark(MetricCollector.Meter.http400);
      throw new ClientException(problem);
    } else if (code >= 400 && code < 500) {
      metricCollector.mark(MetricCollector.Meter.http4xx);
      throw new ClientException(problem);
    } else if (code == 500) {
      metricCollector.mark(MetricCollector.Meter.http500);
      throw new ServerException(problem);
    } else if (code == 503) {
      metricCollector.mark(MetricCollector.Meter.http503);
      throw new ServerException(problem);
    } else if (code > 500 && code < 600) {
      metricCollector.mark(MetricCollector.Meter.http5xx);
      throw new ServerException(problem);
    } else {
      metricCollector.mark(MetricCollector.Meter.httpUnknown);
      throw new HttpException(problem);
    }
  }
}