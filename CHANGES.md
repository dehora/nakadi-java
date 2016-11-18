### Changes

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

