
**Status**

- Build: [![CircleCI](https://circleci.com/gh/zalando-incubator/nakadi-java.svg?style=svg)](https://circleci.com/gh/zalando-incubator/nakadi-java)
- Release Download: [ ![Download](https://api.bintray.com/packages/dehora/maven/nakadi-java-client/images/download.svg) ](https://bintray.com/dehora/maven/nakadi-java-client/_latestVersion)
- Source Release: [0.9.7](https://github.com/zalando-incubator/nakadi-java/releases/tag/0.9.7)
- Contact: [maintainers](https://github.com/zalando-incubator/nakadi-java/blob/master/MAINTAINERS)



# nakadi-java

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *[DocToc](https://github.com/thlorenz/doctoc)*

- [About](#about)
  - [Background](#background)
- [Requirements and Getting Started](#requirements-and-getting-started)
- [Status](#status)
- [Usage](#usage)
  - [Available Resources](#available-resources)
  - [Creating a client](#creating-a-client)
    - [Authorization](#authorization)
    - [OAuth Scopes](#oauth-scopes)
    - [HTTPS Security](#https-security)
    - [Metric Collector](#metric-collector)
    - [JSON](#json)
    - [Using TypeLiterals](#using-typeliterals)
    - [Resource Classes](#resource-classes)
    - [Retries](#retries)
  - [Event Types](#event-types)
  - [Producing Events](#producing-events)
  - [Subscriptions](#subscriptions)
  - [Consuming Events](#consuming-events)
    - [Named Event Type Streaming](#named-event-type-streaming)
    - [Subscription Streaming](#subscription-streaming)
    - [Streaming and Compression](#streaming-and-compression)
    - [Backpressure and Buffering](#backpressure-and-buffering)
  - [Healthchecks](#healthchecks)
  - [Registry](#registry)
  - [Metrics](#metrics)
- [Installation](#installation)
  - [Maven](#maven)
  - [Gradle](#gradle)
  - [SBT](#sbt)
- [Idioms](#idioms)
  - [Fluent](#fluent)
  - [Iterable pagination](#iterable-pagination)
  - [HTTP Requests](#http-requests)
  - [Exceptions](#exceptions)
- [Build and Development](#build-and-development)
- [Internals](#internals)
- [Contributing](#contributing)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
----


## About

Nakadi-java is a client driver for the [Nakadi Event Broker](https://github.com/zalando/nakadi). It was created for the following reasons:

- Completeness. Provide a full reference implementation of the Nakadi API for producers and consumers.

- Minimise dependencies. The client doesn't force a dependency on frameworks or libraries. The sole dependency is on the SLF4J API.

- Robust HTTP handling. Request/response behaviour and consumer stream handling are given the same importance as functionality. 

- Operational visibility. Error handling, stream retries, logging and instrumentation are given the same importance as functionality. 

- Be easy to use. The client should be straighforward to use as is, or as an engine for higher level abstractions.

### Background

A number of JVM clients already exist and are in use - nakadi-java is not meant 
to compete with or replace them. In certain respects they solve different 
goals. The existing JVM clients looked at as a whole, provide partial 
implementations with larger dependencies, but which are idiomatic to certain 
frameworks, whereas the aim of nakadi-java is to provide a full client with a 
reduced dependency footprint to allow portability. 

Nakadi-java is designed for application development. If you're just looking  
for a quick way to browse and examine streams, take a look at the excellent 
[Peek library](https://github.com/zalando-incubator/peek).

## Requirements and Getting Started

See the [installation section](#installation) on how to add the client library 
to your project as a jar dependency. The client uses Java 1.8 or later. 

## Status

The client is pre 1.0.0, with the aim of getting to 1.0.0 quickly. 

The client API is relatively stable and unlikely to see massive sweeping 
changes, though some changes should be expected. The entire Nakadi API is 
implemented.

The client's had some basic testing to verify it can handle things like 
consumer stream connection/network failures and retries. It should not be 
deemed robust yet, but it is a goal to produce a well-behaved production 
level client especially for producing and consuming events for 1.0.0. 
See also:

- The [open issues](https://github.com/zalando-incubator/nakadi-java/issues) section has a 
list of bugs and things to get done. 

- The [help-wanted](https://github.com/zalando-incubator/nakadi-java/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement) has a list of things that would be pretty cool to have.

As a client that aims to provide a full implementation, it will post 1.0.0 
continue to track the development of the Nakadi Event Broker's API.

## Usage

This section summarizes what you can do with the client. The [nakadi-java-examples](https://github.com/zalando-incubator/nakadi-java-examples) project provides runnable examples for most of what you see here.

### Available Resources

The API resources this client supports are:

- [Event Types](#event-types)
- [Events](#producing-events)
- [Subscriptions](#subscriptions)
- [Streams](#consuming-events)
- [Registry](#registry)
- [Healthchecks](#healthchecks)
- [Metrics](#metrics)

### Creating a client

A new client can be created via a builder: 

```java
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .build();
```

You can create multiple clients if you wish. Every client must have a base URI 
set and can optionally have other values set (notably for token providers and 
metrics collection). 

Here's a fuller configuration:

```java
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .metricCollector(myMetricsCollector)
  .tokenProvider(myResourceTokenProvider)
  .readTimeout(60, TimeUnit.SECONDS)
  .connectTimeout(30, TimeUnit.SECONDS)
  .build();
```

#### Authorization

By default the client does not send an authorization header with each request.
This is useful for working with a development Nakadi server, which will try 
and resolve bearer tokens if they are sent but will accept requests with no 
bearer token present. 

You can define a token provider by implementing the `TokenProvider` 
interface, which will supply the client with a string that will be 
sent to the server as the value of an Authorization header. The 
`TokenProvider` is  called on each request and thus can be implemented as 
a dynamic provider to handle token refreshes and recycling.

```java
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .tokenProvider(new MyTokenProvider())
  .build();
```

There's a `ZignTokenProvider` that can connect to the zign process and run in 
the background in the 
[nakadi-java-zign](https://github.com/zalando-incubator/nakadi-java/tree/master/nakadi-java-zign) 
sub-project.

#### OAuth Scopes

Some resources support use of OAuth scopes (where the API documents them, it's incomplete as of 
2016-11-15):

- `StreamProcessor`: can be set via `StreamProcessor.Builder.scope()` before calling `start()`.
- `EventTypeResource`: can be set via `EventTypeResource.scope()` before making an API call.
- `EventResource`: can be set via `EventResource.scope` before making an API call.
- `SubscriptionResource`: can be set via `EventResource.scope` before making an API call.

On each request the client will resolve the scope to a token by asking the `TokenProvider` to 
supply a token via `authHeaderValue`. If a custom scope has been applied on the request it will 
be used, otherwise the default scope documented by the API will be used. 

The scope set on resource instances is stateful, not one-shot, and will be re-used across requests. 
To change the scope, call `scope()` again will a new scope value, or if you wish to clear the 
 custom scope and revert to defaults, call `scope()` with `null`. However the `StreamProcessor` 
 scope is fixed once streaming begins after `start()` is called and can't be changed.
 
 
#### HTTPS Security

The client checks certificates. If your target server is using a self-signed 
certificate and for some reason you can't install that cert into the system 
trust store using something like keytool, you can supply the cert via 
the builder's `certificatePath` method: 

```java
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .certificatePath("file:///var/certs")
  .build();
```

This will cause the client to install any certificates it finds. There are three 
loading options

- A path beginning with `"file:///"` will load from the supplied directory any 
files with `*.crt` and `*.pem` extensions

- A path beginning with `"classpath:"` and ending with `*.crt` or `*.pem` will 
load that resource item from the classpath. 
 
- A path beginning with `"classpath:"` will load from the supplied classpath 
directory any files with `*.crt` and `*.pem` extensions.


The classpath option targeting a directory is for local development and not meant 
for production/deployed situations. If you must use the classpath for deployed apps, 
use the cert resource option as that will allow the classpath resolver to work more 
 generally.

If no `certificatePath` is supplied, the system defaults are used. This is the 
strongly recommended option for deployments.

#### Metric Collector

The client emits well known metrics as meters and timers (see `MetricCollector` 
for the available metrics). 

By default the client ignores metrics, but you can supply your own collector. 
For example, this sets the client to use `MetricsCollectorDropwizard`, from 
the support library that integrates with 
[Dropwizard Metrics](http://metrics.dropwizard.io/3.1.0/):

```java
MetricRegistry metricRegistry = new MetricRegistry();
MetricsCollectorDropwizard metrics =
    new MetricsCollectorDropwizard("mynamespace", metricRegistry);
 
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .metricCollector(metrics)
  .build();
```

To provide your own collector implement the `MetricCollector` interface. Each 
emitted metric is based on an enum. Implementations can look at the enum and 
record as they wish. They can also work with them generally and ask any enum 
for its path, which will be a dotted string.

Please note that calls to the collector are currently blocking. This may be 
changed to asynchronous for 1.0.0, but in the meantime if your collector is 
making network calls or hitting disk, you might want to hand off them off 
as Callables or send them to a queue.

#### JSON

Some calls return `Response` objects that contain raw json. You can serialize 
these using the `JsonSupport` helper, available from the client. `JsonSupport` 
accepts classes, and for generic bindings you can supply it with a `TypeLiteral`.

#### Using TypeLiterals

When using a `TypeLiteral`, please note the following:

- TypeLiterals must be an actual subclass. This means declaring the TypeLiteral with a 
pair of braces `new TypeLiteral<Map<String, Object>>() {};` and not just 
`new TypeLiteral<Map<String, Object>>();`. The latter won't work and can cause hard to debug 
 errors.

- TypeLiterals for the 3 category classes can't be declared with a String. For example 
`DataChangeEvent<String>` will cause marshalling errors, because the underlying JSON 
processing treats `String` as a JSON String type and not escaped JSON. The parser then 
fails when it sees structured JSON instead of a JSOn String. Typically you want to declare 
something like `DataChangeEvent<Map<String, Object>>` to destructure the data properly. The 
client might add a stringified option for 1.0.0.

#### Resource Classes

Once you have a client, you can access server resources via the `resources()` 
method. Here's an example that gets an events resource:

```java 
EventResource resource = client.resources().events();
```

All calls you make to the server will be done via these resource classes to 
make network calls distinct from local requests.

#### Retries

A number of the non streaming resource classes support a backoff policy:

- `EventTypeResource`
- `SubscriptionResource`
- `EventResource`
- `RegistryResource`
- `MetricsResource`
- `HealthCheckResource`

They each take a `RetryPolicy` via a `retryPolicy()` method; there is an inbuilt `ExponentialRetry` 
that can be used to define a maximum number of requests or maximum total time elapsed. Note that 
the retry policy object is stateful and must be reset between results. You can disable the retries
 (the default behavior) by setting `retryPolicy` to null, or to start a new retry supplying a fresh 
 `RetryPolicy` instance.  

**Please be careful with EventTypeResource**: the ordering and general delivery behaviour for event 
delivery is **undefined** under retries. That is, a delivery retry may result in out of order 
batches being sent to the server. Also retrying a partially delivered (207) batch may result 
in one or more events being delivered multiple times. 

### Event Types

You can create, edit and delete event types as well as list them:

```java
// grab an event type resource
EventTypeResource eventTypes = client.resources().eventTypes();
 
// create a new event type, using an escaped string for the schema
EventType requisitions = new EventType()
  .category(EventType.Category.data)
  .name("priority-requisitions")
  .owningApplication("weyland")
  .partitionStrategy(EventType.PARTITION_HASH)
  .enrichmentStrategy(EventType.ENRICHMENT_METADATA)
  .partitionKeyFields("id")
  .schema(new EventTypeSchema().schema(
      "{ \"properties\": { \"id\": { \"type\": \"string\" } } }"));
Response response = eventTypes.create(requisitions);
 
// read the partitions for an event type
PartitionCollection partitions = eventTypes.partitions("priority-requisitions");
partitions.iterable().forEach(System.out::println);
 
// read a particular partition
Partition partition = eventTypes.partition("priority-requisitions", "0");
System.out.println(partition);
 
// list event types
EventTypeCollection list = client.resources().eventTypes().list();
list.iterable().forEach(System.out::println);
 
// find by name 
EventType byName = eventTypes.findByName("priority-requisitions");
 
// update 
Response update = eventTypes.update(byName);
 
// remove 
Response delete = eventTypes.delete("priority-requisitions");
```

### Producing Events

You can send one or more events to the server:

```java
EventResource resource = client.resources().events();
 
// nb: EventMetadata sets defaults for eid, occurred at and flow id fields
EventMetadata em = new EventMetadata();
  .eid(UUID.randomUUID().toString())
  .occurredAt(OffsetDateTime.now())
  .flowId("decafbad");
  
// create our domain event inside a typesafe DataChangeEvent  
PriorityRequisition pr = new PriorityRequisition("22");
DataChangeEvent<PriorityRequisition> dce = new DataChangeEvent<PriorityRequisition>()
  .metadata(em)
  .op(DataChangeEvent.Op.C)
  .dataType("priority-requisitions")
  .data(pr);
 
Response response = resource.send("priority-requisitions", dce);
 
// send a batch of two events
 
DataChangeEvent<PriorityRequisition> dce1 = new DataChangeEvent<PriorityRequisition>()
  .metadata(new EventMetadata())
  .op(DataChangeEvent.Op.C)
  .dataType("priority-requisitions")
  .data(new PriorityRequisition("23"));
 
DataChangeEvent<PriorityRequisition> dce2 = new DataChangeEvent<PriorityRequisition>()
  .metadata(new EventMetadata())
  .op(DataChangeEvent.Op.C)
  .dataType("priority-requisitions")
  .data(new PriorityRequisition("24"));

ArrayList list = new ArrayList();
list.add(dce1);
list.add(dce2);
 
Response batch = resource.send("priority-requisitions", list);
```

### Subscriptions

You can create, edit and delete subscriptions as well as list them:

```java
// grab a subscription resource
SubscriptionResource resource = client.resources().subscriptions();
 
// create a new subscription
Subscription subscription = new Subscription()
    .consumerGroup("mccaffrey-cg")
    .eventType("priority-requisitions")
    .owningApplication("shaper");
 
Response response = resource.create(subscription);

// create a subscription from a given offset
Cursor c0 = new Cursor("0", "000000000000002009", "priority-requisitions");
Cursor c1 = new Cursor("1", "000000000000002008", "priority-requisitions");

Subscription offsetSubscription = new Subscription()
    .consumerGroup("roja-cg")
    .eventType("priority-requisitions")
    .owningApplication("anarch")
    .readFrom("cursors")
    .initialCursors(Lists.newArrayList(c0, c1));    

// find a subscription
Subscription found = resource.find("a2ab0b7c-ee58-48e5-b96a-d13bce73d857");
 
// get the cursors and iterate them
SubscriptionCursorCollection cursors = resource.cursors(found.id());
cursors.iterable().forEach(System.out::println);
 
// get the stats and iterate them
SubscriptionEventTypeStatsCollection stats = resource.stats(found.id());
stats.iterable().forEach(System.out::println);
 
// list subscriptions
SubscriptionCollection list = resource.list();
list.iterable().forEach(System.out::println);
 
// list for an owner
list = resource.list(new QueryParams().param("owning_application", "shaper"));
list.iterable().forEach(System.out::println);
 
// delete a subscription
Response delete = resource.delete(found.id());
```

### Consuming Events

You can consume events via stream. Both the named event type and newer 
subscription stream APIs are available via the `StreamProcessor` class.

A `StreamProcessor` accepts a `StreamObserverProvider` which is a factory for 
creating the `StreamObserver` class the events will be sent to. The 
`StreamObserver` accepts one or more `StreamBatchRecord` objects  where each 
item in the batch has been marshalled to an instance of `T` as defined by 
it and the `StreamObserverProvider`.  

A `StreamObserver` implements a number of callback methods that are invoked 
by the underlying stream processor:

- `onStart()`:  Called before stream connection begins and before a retry is attempted.

- `onStop()`: Called after the stream is completed and when a retry is needed.

- `onCompleted()`: Called when the client is finished sending batches.

- `onError(Throwable t)`: Called when there's been an error.

- `onNext(StreamBatchRecord<T> record)`: Called for each batch of events. Also contains the current offset observer and the batch cursor.

- `requestBackPressure()`: request a maximum number of emitted items from the stream. 

- `requestBuffer()`: Ask to have batches buffered before emitting them from the stream.

The interface is influenced by [RxJava](https://github.com/ReactiveX/RxJava) 
and the general style of `onX`  callback APIs. You can see an example in the 
source called `LoggingStreamObserverProvider` which maps the events in a 
batch to plain strings.

The API also supports a `StreamOffsetObserver` - the offset observer is given 
to the `StreamObserver` object with each `onNext` call. Typically the offset 
observer is used to provide checkpointing of a consumer's partition in the 
stream. 

#### Named Event Type Streaming

To consume a named event type stream, configure a `StreamProcessor` and run it:

```java

// configure a stream for an event type from a given cursor; 
// all api settings are available
StreamConfiguration sc = new StreamConfiguration()
    .eventTypeName("priority-requisitions")
    .cursors(new Cursor("0", "450"));

// set up a processor with an event observer provider
StreamProcessor processor = client.resources().streamBuilder()
    .streamConfiguration(sc)
    .streamObserverFactory(new LoggingStreamObserverProvider())
    .build();

// consume in the background until the app exits or stop() is called
processor.start(); 

// configure a stream with a bounded number of events retries, keepalives, plus custom timeouts
StreamConfiguration sc1 = new StreamConfiguration()
    .eventTypeName("priority-requisitions")
    .cursors(new Cursor("0", "450"))
    .batchLimit(15)
    .batchFlushTimeout(2, TimeUnit.SECONDS)
    .maxRetryAttempts(256)
    .maxRetryDelay(30, TimeUnit.SECONDS)
    .streamLimit(1024)
    .connectTimeout(8, TimeUnit.SECONDS)
    .readTimeout(3, TimeUnit.MINUTES)
    .streamKeepAliveLimit(2048)
    .streamTimeout(1, TimeUnit.DAYS);
 
// create a processor with an observer and an offset observer  
StreamProcessor boundedProcessor = client.resources().streamBuilder()
    .streamConfiguration(sc1)
    .streamObserverFactory(new LoggingStreamObserverProvider())
    .streamOffsetObserver(new LoggingStreamOffsetObserver())
    .build();
 
/*
 start in the background, stopping when the criteria are reached,
 the app exits, or stop() is called
*/
boundedProcessor.start(); 
```

If no offset observer is given, the default observer used is 
`LoggingStreamOffsetObserver` which simply logs when it is invoked.


#### Subscription Streaming

Subscription stream consumers allow consumers to store offsets with the server 
and work much like named event type streams:

```java
// configure a stream from a subscription id; 
// all api settings are available
StreamConfiguration sc = new StreamConfiguration()
    .subscriptionId("27302800-bc68-4026-a9ff-8d89372f8473")
    .maxUncommittedEvents(20L);

// create a processor with an observer
StreamProcessor processor = client.resources().streamBuilder(sc)
    .streamObserverFactory(new LoggingStreamObserverProvider())
    .build();

// consume in the background until the app exits or stop() is called
processor.start();
```

There are some notable differences: 

- The `StreamConfiguration` is configured with a `subscriptionId`  instead of an `eventTypeName`.

- The inbuilt offset observer for a subscription stream will call Nakadi's checkpointing API to update the offset. You can replace this with your own implementation if you wish.

- A subscription stream also allows setting the `maxUncommittedEvents` as defined by the Nakadi API.

#### Streaming and Compression

The default behaviour for all streaming consumers is to request a gzipped stream. This can 
be changed to a plain stream by setting the `Accept-Encoding` header to `identity` on 
 `StreamConfiguration` as follows:

```java
StreamConfiguration sc = new StreamConfiguration()
    //  ask the server for unencoded data
    .requestHeader("Accept-Encoding", "identity")
    ...;
```

#### Backpressure and Buffering

A `StreamObserver` can signal for backpressure via the `requestBackPressure` 
method. This is applied with each `onNext` call to the `StreamObserver` and 
so can be used to adjust backpressure dynamically. The client's underlying 
stream processor will make a  best effort attempt to honor backpressure.

If the user wants events buffered into contiguous batches it can set a buffer 
size using `requestBuffer`. This is independent of the underlying HTTP 
stream - the stream will be consumed off the wire based on the API request 
settings - the batches are buffered in memory by the underlying processor. 
This is applied during setup and is fixed for the processor's lifecycle.

Users that don't care about backpresure controls can subclass the
 `StreamObserverBackPressure` class.


### Healthchecks

You can make healthcheck requests to the server:

```java
HealthCheckResource health = client.resources().health();
 
// check returning a response object, regardless of status
Response healthcheck = health().healthcheck();
 
// ask to throw if the check failed (non 2xx code)
Response throwable = health.healthcheckThrowing();

// check with an expoential backoff retry

RetryPolicy retry = ExponentialRetry.newBuilder()
        .initialInterval(1000, TimeUnit.MILLISECONDS)
        .maxAttempts(5)
        .maxInterval(3000, TimeUnit.MILLISECONDS)
        .build();
health.retryPolicy(retry).healthcheckThrowing();
```

### Registry

You can view the service registry:

```java
RegistryResource resource = client.resources().registry();
 
// get and iterate available enrichments
EnrichmentStrategyCollection enrichments = resource.listEnrichmentStrategies();
enrichments.iterable().forEach(System.out::println);
 
// get and iterate available validations
ValidationStrategyCollection validations = resource.listValidationStrategies();
validations.iterable().forEach(System.out::println);        
```

### Metrics

You can view service metrics:

```java
MetricsResource metricsResource = client.resources().metrics();
 
// print service metrics
MetricsResource metricsResource = client.resources().metrics();
Metrics metrics = metricsResource.get();
Map<String, Object> items = metrics.items();
System.out.println(items);
```

Note that the structure of metrics is not defined by the server, hence it's 
returned as as map within the `Metrics` object.

## Installation

### Maven

Add jcenter to the repositories element in `pom.xml` or `settings.xml`:

```xml
<repositories>
  <repository>
    <id>jcenter</id>
    <url>http://jcenter.bintray.com</url>
  </repository>
</repositories>
```  

and add the project declaration to `pom.xml`:

```xml
<dependency>
  <groupId>net.dehora.nakadi</groupId>
  <artifactId>nakadi-java-client</artifactId>
  <version>0.9.7</version>
</dependency>
```
### Gradle

Add jcenter to the `repositories` block:

```groovy
repositories {
 jcenter()
}
```

and add the project to the `dependencies` block in `build.gradle`:

```groovy
dependencies {
  compile 'net.dehora.nakadi:nakadi-java-client:0.9.7'
}  
```

### SBT

Add jcenter to `resolvers` in `build.sbt`:

```scala
resolvers += "jcenter" at "http://jcenter.bintray.com"
```

and add the project to `libraryDependencies` in `build.sbt`:

```scala
libraryDependencies += "net.dehora.nakadi" % "nakadi-java-client" % "0.9.7"
```


## Idioms

### Fluent

The client prefers a fluent style, setters return `this` to allow chaining. 
Complex constructors use a builder pattern where needed. The JavaBeans 
get/set prefixing idiom is not used by the API, as is increasingly typical 
with modern Java code.

### Iterable pagination

Any API call that returns a collection, including ones that could be paginated 
expose Iterable contracts, allowing `forEach` or `iterator` access:

```java 
EventTypeCollection list = client.resources().eventTypes().list();
list.iterable().forEach(System.out::println);
 
Iterator<EventType> iterator = list.iterable().iterator();
while (iterator.hasNext()) {
  EventType next = iterator.next();
  System.out.println(next);
}
```

Pagination if it happens, is done automatically by the collection's backing 
iterable by following the `next` relation sent back by the server. 

You can if wish work with pages and hypertext links directly via the methods 
on `ResourceCollection` which each collection implements.

### HTTP Requests

Calls that result in HTTP requests are performed using resource classes. The 
results can be accessed as HTTP level responses or mapped to API objects.

You don't have to deal with HTTP responses from the API directly. If there 
is a failure then a `NakadiException` or a subclass will be thrown. The 
exception will have `Problem` information that can be examined. 

### Exceptions

Client exceptions are runtime exceptions by default. They extend from 
`NakadiException` which allows you to catch all errors under one type. The 
`NakadiException` embeds a `Problem` object which can be examined. Nakadi's 
API uses Problem JSON ([RFC7807](https://tools.ietf.org/html/rfc7807)) to 
describe errors. Local errors also contain Problem descriptions. 

The client will also throw an `IllegalArgumentException` in a number of places 
where null fields are not accepted or sensible as values, such as required 
parameters for builder classes. However the client performs no real data 
validation for API requests, leaving that to the server. Invalid server 
requests resulting in 422s will cause an `InvalidException` to be thrown 
instead.

In a handful of circumstances the API exposes a checked exception where 
it's neccessary the user handles the error; for example some exceptions 
from `StreamOffsetObserver` are checked.

## Build and Development

The project is built with [Gradle](http://gradle.org/) and uses the 
[Netflix Nebula](https://nebula-plugins.github.io/) plugins. The `./gradlew` 
wrapper script will bootstrap the right Gradle version if it's not already 
installed. 

The main client jar file is build using the shadow plugin.

The main tasks are:

- `./gradlew build` : run a build and test
- `./gradlew clean` : clean down the build 
- `./gradlew clean shadow` : builds the client jar

## Internals

The wiki page [Internals](https://github.com/dehora/nakadi-java/wiki/Internals)
has details on how the client works under the hood.

## Contributing

Please see the [issue tracker](https://github.com/zalando-incubator/nakadi-java/issues) 
for things to work on. The [help-wanted](https://github.com/zalando-incubator/nakadi-java/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement) has a list of things that would be 
pretty cool to have.

Before making a contribution, please let us know by posting a comment to the 
relevant issue. If you would like to propose a new feature, create a new issue 
first explaining the feature you’d like to contribute or bug you want to fix.

The codebase follows [Square's code style](https://github.com/square/java-code-styles) 
for Java and Android projects.

----

## License

MIT License

Copyright (c) 2016 Zalando SE, https://tech.zalando.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


