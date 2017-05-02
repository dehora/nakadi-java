### Changes

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

