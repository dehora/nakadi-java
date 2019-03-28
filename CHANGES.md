### Changes

### 0.13.0

- Adds circleci 2 configuration.
- Adds support for sending authorizations with subscriptions. @lmontrieux 

### 0.12.0

- Adds support for propagating tracing spans. [#336](https://github.com/dehora/nakadi-java/issues/336)


### 0.11.0

- Strips trailing '/' from arg supplied to UriBuilder.builder. [#330](https://github.com/dehora/nakadi-java/pull/330)

### 0.10.0

- Adds support for creating log compacted event types. [#329](https://github.com/dehora/nakadi-java/pull/328)

### 0.9.17

This promotes 0.9.17.b1 and 0.9.17.b2.

- Allows a consumer to see the checkpoint response result from the server.
- Allows the request write timeout to be set. [#307](https://github.com/zalando-incubator/nakadi-java/pull/307)
- Fixes stop conjunction logic in StreamProcessor [#304](https://github.com/zalando-incubator/nakadi-java/pull/304), @PetrGlad

### 0.9.17.b2

This is a beta release.

- Allows a consumer to see the checkpoint response result from the server.

### 0.9.17.b1

This is a beta release.

- Allows the request write timeout to be set. [#307](https://github.com/zalando-incubator/nakadi-java/pull/307)
- Fixes stop conjunction logic in StreamProcessor [#304](https://github.com/zalando-incubator/nakadi-java/pull/304), @PetrGlad

### 0.9.16

- Makes SubscriptionEventTypeStatsCollection constructor visible for testing.

### 0.9.15

- promotes 0.9.15.b2 to 0.9.15

### 0.9.15.b2

This is a beta release.

- Adds a guard clause to avoid calling onStart when processor is stopping.
- Removes logging noise. 

### 0.9.15.b1

This is a beta release.

- Removes custom buffering from stream processor. [#294](https://github.com/dehora/nakadi-java/issues/294)
- Tidies stream processor logging and reduces allocations.

### 0.9.14

- Stops the processor after a successful response with a stream timeout.[#291](https://github.com/dehora/nakadi-java/issues/291)
- Calls StreamObserver.onStop only on termination. [#289](https://github.com/dehora/nakadi-java/issues/289)

### 0.9.13

- Lowers default batch buffer and allows setting by configuration. [#256](https://github.com/dehora/nakadi-java/issues/256)

### 0.9.12

- Stops stream processor after retries are exceeded.  [#284](https://github.com/dehora/nakadi-java/issues/284)
- Avoids StreamProcessor.stop waiting on start completion.
- Tidies up OkHttpResource args and connection closing.
- Removes leap second check on event times.

### 0.9.11

- Reduce allocations when marshalling events from a stream. [#256](https://github.com/dehora/nakadi-java/issues/256)

### 0.9.10

- Adds a close method to the Response interface. [#237](https://github.com/dehora/nakadi-java/issues/237)

### 0.9.9

- Routes  undeliverable exceptions to a handler.  [#225](https://github.com/dehora/nakadi-java/issues/225)
- Removes duplicate Observer onStart call and adds an onBegin call.
- Reduces default buffer size on stream processor when consumers can't keep up from 16K to 8K.

### 0.9.8

- Excludes test jar from nakadi-java-zign. [#223](https://github.com/dehora/nakadi-java/issues/223)

### 0.9.7

- Makes publishing status and steps visible in BatchItemResponse. [#264](https://github.com/dehora/nakadi-java/issues/264)
- Deprecates scopes. [#228](https://github.com/dehora/nakadi-java/issues/228)
- Adds authorizations to event types. [#227](https://github.com/dehora/nakadi-java/issues/227)

### 0.9.6

- Adds IllegalStateException to non-retryable consumer errors.
- Allows detecting if a StreamProcessor is running.
- Allows tracking/identification of errors thrown from StreamProcessor that caused it to stop.
- Cleans up observer error/exception handling for errors and non-retryable exceptions.

### 0.9.5
- Fixes log message for cursor updates. [#244](https://github.com/dehora/nakadi-java/issues/244) 
- Errors thrown inside processor schedulers are propagated. [243](https://github.com/dehora/nakadi-java/issues/243)
- Sends flat JSON when posting events. [#236](https://github.com/dehora/nakadi-java/issues/236)
- Improved processor shutdown  [#225](https://github.com/dehora/nakadi-java/issues/225), [#222](https://github.com/dehora/nakadi-java/issues/222)

### 0.9.4

- Documents disabling compression on consumer streams.
- Allows headers to be sent with posted events. [#238](https://github.com/dehora/nakadi-java/issues/238)

### 0.9.3

- Allows setting event type statistics on event creation.

### 0.9.2

- Fixes setting maxRetry attempts for stream configuration. [#231](https://github.com/dehora/nakadi-java/issues/231)

### 0.9.1

- Mark 404 response codes as non-retryable. [#226](https://github.com/zalando-incubator/nakadi-java/issues/226)

### 0.9.0

Contains breaking changes relative to 0.8.x.

- Breaking change. Removes deprecated means to suppress invalid exceptions.
- Breaking change. Prevents stream processor start being called after stop.
- Allow X-Flow-ID header to be specified by application. [#193](https://github.com/zalando-incubator/nakadi-java/issues/193)
- Fixes stream processor shut down.  [#186](https://github.com/zalando-incubator/nakadi-java/issues/186)
- Observer onError is only called when the consumer pipeline is going to stop processing.
- Updates Observer documentation to reflect consumer retry policy.
- Breaking change. Defaults stream processor to retry exceptions except for Errors and NonRetryableNakadiException.

### 0.8.8

- Checks if the next pagination url is relative. [#204](https://github.com/zalando-incubator/nakadi-java/issues/204)
- Fixes JSON field name for event type statistics. [#203](https://github.com/zalando-incubator/nakadi-java/issues/203)


### 0.8.7

- Allows JSON support to be supplied via the `JsonSupport` interface. [#182](https://github.com/zalando-incubator/nakadi-java/issues/182)
- Adds an extension jar, "nakadi-java-gson", for `JsonSupport` via Gson. [#182](https://github.com/zalando-incubator/nakadi-java/issues/182) 

### 0.8.6

- More workarounds for [zalando/nakadi#645](https://github.com/zalando/nakadi/issues/645). [#188](https://github.com/zalando-incubator/nakadi-java/issues/188)

### 0.8.5

- Adds support for cursor reset API.
- Reduces visibility of Resource interface.
- Internal refactorings and cleanups.

### 0.8.4 

- Removes event type API lookup used for logging subscription consumer details.
- Close stream response bodies at the stream iterator layer to avoid lingering connections.
- Tidies up stream consumer logging with easier to grep strings.

### 0.8.3 


- Avoids nulls when logging checkpoint suppressions.

### 0.8.2

- Handles and cleans up after observer exceptions.
- Handles missing response entities from server.
- Works around auth structure problems in https://github.com/zalando/nakadi/issues/645.

### 0.8.1

- Allows network errors on checkpoint requests to be suppressed. 

### 0.8.0

- Promotes 0.7.13 to 0.8.0

### 0.7.13

- Coerces raw string event posting to UTF-8. [124](https://github.com/zalando-incubator/nakadi-java/issues/124)
- Allows a specific cert to be loaded from the classpath. [126](https://github.com/zalando-incubator/nakadi-java/issues/126)
- Fixes integers in BusinessEventMapped being serialised to floats. [119](https://github.com/zalando-incubator/nakadi-java/issues/119)

### 0.7.12

- Adds support for requesting `consumed_offset` per partition. [155](https://github.com/zalando-incubator/nakadi-java/issues/155)
- Adds support for requesting cursor lag. [153](https://github.com/zalando-incubator/nakadi-java/issues/153)
- Adds support for requesting cursor shifts. [#152](https://github.com/zalando-incubator/nakadi-java/issues/152)
- Adds support for cursor distance checks. [#151](https://github.com/zalando-incubator/nakadi-java/issues/151)

### 0.7.11

- Allows batch offset data to be routed to one or more subscribers.
- Allows the checkpointer to be provided in configuration.
- Allows 422 exceptions from offset commits to be suppressed [#117](https://github.com/zalando-incubator/nakadi-java/pull/117).
- Adds a marker interface, `@Unstable` for candidate API classes and methods.
- Extracts the subscription checkpoint API call to a utility checkpointer class.

### 0.7.10

- Fixes retry handling preventing per stream event batches being emitted to observers.
- Exposes number of retries to date in RetryPolicy.
- Fixes retry delay timer to honor backoff times.

### 0.7.9

- Supports initial_cursors for creating Subscriptions. [#139](https://github.com/zalando-incubator/nakadi-java/pull/139)
- Upgrades stream processors and resources to RxJava2.
- Upgrades OkHttp to 3.7.0

### 0.7.8

- Ignores batch limit values set to 0 or less. [#125](https://github.com/zalando-incubator/nakadi-java/pull/125)

### 0.7.7

- Fixes generic event types: Event becomes Event<T> [#130](https://github.com/zalando-incubator/nakadi-java/pull/130), @aakavalevich

### 0.7.6

- Tests behaviour for Accept-Encoding: gzip from consumers. [#127](https://github.com/zalando-incubator/nakadi-java/issues/127)

### 0.7.5

- Handle batch item responses for event posting. [#116](https://github.com/zalando-incubator/nakadi-java/issues/116)

### 0.7.4

- Add support for posting raw JSON strings.

### 0.7.3

- Add experimental support for schema versioning to track the Nakadi API.

### 0.7.2

- Capture metrics and add more logging for checkpoint requests.
- Use a new http logger that elides auth and log to SLF4J

### 0.7.1

- Give StreamProcessor scheduler threads specific names.
- Block the caller on StreamProcessor.startBlocking, mark method as deprecated

### 0.7.0

Rollup of 0.6.x releases

- Add uncaught exception loggers to thread factories.
- Add type literal helper support.
- Fix request/3 to use non-throwing http call.
- Handle retries with non-throwing requests.
- Change streaming accept header to application/json [#98](https://github.com/zalando-incubator/nakadi-java/issues/98)
- Add back pressure buffering to stream processor for volume stream requests.  [#100](https://github.com/zalando-incubator/nakadi-java/issues/100)
- Allow blocking access to stream processors

### 0.6.3

- Add back pressure buffering to stream processor for volume stream requests.  [#100](https://github.com/zalando-incubator/nakadi-java/issues/100)

### 0.6.2

- Change streaming accept header to application/json [#98](https://github.com/zalando-incubator/nakadi-java/issues/98)

### 0.6.1

- Allow blocking access to streams

### 0.6.0

- Reject stream retry delays that are less than 1s [#83](https://github.com/zalando-incubator/nakadi-java/issues/83)
- Extend RetryPolicy to support a max time backoff [#81](https://github.com/zalando-incubator/nakadi-java/issues/81)
- Add some optional (non-throwing on 404) finders to event types and subscriptions [#71](https://github.com/zalando-incubator/nakadi-java/issues/71)

### 0.5.0

- Fix zign process call
- TokenProviderZign adds known scopes by default [#78](https://github.com/zalando-incubator/nakadi-java/issues/78)
- Update readme to point at nakadi-java-examples, help wanted, and recent features

### 0.4.0

- Make JsonSupport visible [#73](https://github.com/zalando-incubator/nakadi-java/issues/73)
- Add serialized subscription finder [#72](https://github.com/zalando-incubator/nakadi-java/issues/72)

### 0.3.0

- Don't use exhausted (finished) retries, add a meter to track when this happens
- Fix bug in throwing request with serialised response on the non-retry path
- Clean up OkHttpResource http call chains, and always run requests inside an observable


### 0.2.0

- Test http resource methods
- Test how business and undefined events are posted over HTTP
- Test retryable exceptions
- Test status code to exception mappings
- Update serialization type for undefined events
- Add Javadoc to the subscription resources
- Handle the case when the server returns no problem

### 0.1.1

- Fix bug serializing raw responses.
- Make ExponentialRetry public.

### 0.1.0

- Add backoff policy support to methods on Resource/OkHttpResource.
- Add backoffs retryable errors on auto-paginators. [#27](https://github.com/zalando-incubator/nakadi-java/issues/27)
- Add backoff support to retryable errors on metrics, registry, health, event type, event and subscription calls.
- Allow callers to set retry policies.


### 0.0.9

- Allow custom scopes to be set for event streaming.
- Allow custom scopes to be set for event sending.
- Allow scopes and custom scopes to be set for event type resources.
- Allow custom scopes to be set for subscriptions.
- Simplify event sending method interface.

#### 0.0.8

- Remove dependencies from TokenProviderZign

#### 0.0.7

- Change the TokenProvider interface to accept a scope; breaking interface change with 0.0.6
- Add an extension jar, "nakadi-java-zign", for Zign tokens [#20](https://github.com/zalando-incubator/nakadi-java/issues/20)
- Make ExecutorServiceSupport public (allows the Zign extension to run in the background)
- Use the event write scope when sending an event
- Use the event read scope when opening an event stream

#### 0.0.6

- Allow self-signed or other non-trusted certificates to be configured [#40](https://github.com/zalando-incubator/nakadi-java/issues/40)

#### 0.0.5

 - Fix non-null check on cursors

#### 0.0.4

- Handle retryable errors on subscription consumer checkpointer [#28](https://github.com/zalando-incubator/nakadi-java/issues/28)
- Run healthcheck with a backoff.
- Extract a common data field interface for all Event categories.
- Fix event type setter checks


#### 0.0.3

- Add Javadoc to API. [#19](https://github.com/zalando-incubator/nakadi-java/issues/19)
- Shutdown all stream processing executors.
- Remove unused class SubscriptionList.
- Publishing to jcenter instead of dl.bintray

#### 0.0.2

- Make UndefinedEventMapped and BusinessEventMapped generic [#18](https://github.com/zalando-incubator/nakadi-java/issues/18)
- Handle half-open connections or the server not sending keepalives within batch limit time. [#21](https://github.com/zalando-incubator/nakadi-java/issues/21)
- Remove the option to configure the JSON provider.
- Handle leap second strings by pushing them back 1 second.
- Improve stream processor and observer logging.
- Rework thread model to use two single threaded io and processing executors.
- Grant copyright to Zalando SE.

#### 0.0.1

- Add an overview to the readme.
- Deprecate setting JSON support (will be rm'd for 1.0.0).
- Provide convenience methods for event posting.
- Use varargs for setting parent eids.

#### 0.0.0

Initial release. Complete implementation of the API with support for: 

- Event Types. Create, edit, delete, list. Read partition information.
- Events. Send one or multiple events.
- Subscriptions. Create, edit, delete, find, list. View cursors.
- Streams. Subscription and Event Type streams. Streams can automatically retry on failure. 
Subscription streams automatically checkpoint. Customer stream consumers can be configured 
with backpressure.
- Registry. View enrichments and validations. Note: reading validations is documented but not 
supported by the Nakadi server.
- Metrics. Read server metrics.
- Health. Exception and HTTP based healthcheck calls.

