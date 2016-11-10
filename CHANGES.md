### Changes

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

