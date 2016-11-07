### Changes

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

#### 0.0.1

- Add an overview to the readme

- Deprecate setting JSON support (will be rm'd for 1.0.0)

- Provide convenience methods for event posting

- Use varargs for setting parent eids


