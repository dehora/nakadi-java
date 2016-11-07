🌀 [![CircleCI](https://circleci.com/gh/dehora/nakadi-java.svg?style=svg&circle-token=441a537c321834aaf46223d017ced8d9d043e5e0)](https://circleci.com/gh/dehora/nakadi-java)

# nakadi-java

----

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *[DocToc](https://github.com/thlorenz/doctoc)*

- [About](#about)
- [Requirements](#requirements)
- [Resources](#resources)
- [Usage](#usage)
  - [Creating a client](#creating-a-client)
    - [Authorization](#authorization)
    - [Metric Collector](#metric-collector)
    - [Resource Classes](#resource-classes)
  - [Event Types](#event-types)
  - [Producing Events](#producing-events)
  - [Subscriptions](#subscriptions)
  - [Consuming Events](#consuming-events)
    - [Named Event Streaming](#named-event-streaming)
    - [Subscription Streaming](#subscription-streaming)
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
  - [Internal Dependencies](#internal-dependencies)
  - [Logging](#logging)
  - [Business and Undefined Event Categories](#business-and-undefined-event-categories)
  - [Stream Processing](#stream-processing)
    - [Batch Consumption](#batch-consumption)
    - [Thread Scheduling](#thread-scheduling)
    - [Observer and Offset Observer Interaction](#observer-and-offset-observer-interaction)
    - [Resumption](#resumption)
- [Contributing](#contributing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
----

## About

Nakadi-java is a client driver for the [Nakadi Event Broker](https://github.com/zalando/nakadi). It was created for the following reasons:

- To provide a complete reference implementation of the Nakadi API for producers and consumers, that can be used as is, or as the engine for higher level abstractions.

- Minimise dependencies. The project doesn't force a dependency on frameworks or libraries. The sole dependency is on the SLFJ4 API.

- To pay attention to production level HTTP handling. Runtime behaviour, error handling, logging and instrumentation matter as much as functionality. 

A number of existing clients already exist and are in use. Nakadi-java is not meant to compete with or replace them. But the existing JVM clients, looked at as a whole provide focused but partial implementations or have larger dependencies. 

## Requirements

Java 1.8 or later. If you want to build the project you'll also need Gradle. 
The client uses SLFJ4 for logging; you may want to provide an implementation 
binding that works with your local setup. 

See the [installation section](#installation) on how to add the client to 
your project.

## Resources

API resources this client supports:

- [Event Types](#event-types)
- [Events](#producing-events)
- [Subscriptions](#subscriptions)
- [Streams](#consuming-events)
- [Registry](#registry)
- [Healthchecks](#healthchecks)
- [Metrics](#metrics)

## Usage

The sections summarizes what you can do with the client. The [nakadi-java-examples](https://github.com/dehora/nakadi-java-examples) project provides runnable examples in Java.

### Creating a client

A new client can be created via a builder: 

```java
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .build();
```

You can create multiple clients if you wish. Every client must have a base URI 
set and can optionally have other values set (notably for token providers and metrics collection). 

Here's a fuller configuration:

```java
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .metricCollector(myMetricsCollector)
  .resourceTokenProvider(myResourceTokenProvider)
  .readTimeout(60, TimeUnit.SECONDS)
  .connectTimeout(30, TimeUnit.SECONDS)
  .build();
```

#### Authorization

The default client does not send an authorization header with each request. This is useful for working with a development Nakadi server, which will 
try and resolve bearer tokens if they are sent but will accept requests 
with no bearer token present. 

You can define a provider by implementing the `ResourceTokenProvider` interface which supplies the client with a `ResourceToken` that will be 
sent to the server as an OAuth Bearer Token. The `ResourceTokenProvider` 
is  considered a factory and called on each request and thus can be implemented as a dynamic provider to handle token refreshes and recycling.

```java
NakadiClient client = NakadiClient.newBuilder()
  .baseURI("http://localhost:9080")
  .resourceTokenProvider(new MyTokenProvider())
  .build();
```

#### Metric Collector

The client emits well known metrics as meters and timers. See `MetricCollector`for the available metrics. The default client ignores metrics, but you can supply your own collector. For example this sets the client to use 
`MetricsCollectorDropwizard`, from the support library that integrates with Dropwizard metrics:

```java
    MetricRegistry metricRegistry = new MetricRegistry();
    MetricsCollectorDropwizard metrics =
        new MetricsCollectorDropwizard("mynamespace", metricRegistry);

    final NakadiClient client = NakadiClient.newBuilder()
        .baseURI("http://localhost:9080")
        .metricCollector(metrics)
        .build();
```

To provide your own collector implement the `MetricCollector` interface. Each emitted metric is based on an enum. Implementations can look at the enum and record as they wish. They can also work with them generally and ask any enum for its path, which will be a dotted string.

Please note that calls to the collector are currently blocking. This may be 
changed to asynchronous for 1.0.0, but in the meantime if your collector is making network calls or hitting disk, you might want to hand off them off 
as Callables or send them to a queue.

#### Resource Classes

Once you have a client, you can access server resources via the `resources()` method. Here's an example that gets an events resource:

```java 
EventResource resource = client.resources().events();
```

All calls you make to the server will be done via these resource classes to make network calls distinct from local requests.

### Event Types

You can create, edit and delete event types as well as list them.

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
 
Response batch = resource.send("priority-requisitions", dce1, dce2);
```

### Subscriptions

You can create, edit and delete susbcriptions as well as list them:

```java
// grab a subscription resource
SubscriptionResource resource = client.resources().subscriptions();
 
// create a new subscription
Subscription subscription = new Subscription()
    .consumerGroup("mccaffrey-cg")
    .eventType("priority-requisitions")
    .owningApplication("shaper");
 
Response response = resource.create(subscription);
 
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

You can consume events via stream. Both the event type and newer subscription 
streams are available.

A stream accepts a `StreamObserverProvider` which is a factory for creating the 
`StreamObserver` class your events will be sent to. The `StreamObserver` accepts 
one or more `StreamBatchRecord` objects  where each item in the batch has been
marshalled to an instance of `T` as defined by it and the
`StreamObserverProvider`.  

A `StreamObserver` implements a number of callback methods that are invoked 
by the underlying stream processor:

- `onStart()`:  Called before stream connection begins and every time a retry is made.

- `onStop()`: Called after the stream is completed and every time a retry fails.

- `onCompleted()`: Notifies the Observer that we're finished sending batches.

- `onError(Throwable t)`: Notifies the Observer we've seen an error.

- `onNext(StreamBatchRecord<T> record)`: give the obsever a batch of events. Also contains the current offset observer and the batch cursor.

- `requestBackPressure()`: request a maximum number of emitted items. 

- `requestBuffer()`: Ask to have batches buffered before emitting them.

The interface is heavily influenced by RxJava and that general style of `onX` 
callback API. You can see an example in the source called
`LoggingStreamObserverProvider` which maps the events in a batch to plain 
strings.

The API also supports a `StreamOffsetObserver` - the offset observer is given to 
the `StreamObserver` object. Typically the offset observer is used to provide 
checkpointing of a consumer's partition in the stream. If no offset observer is 
given, the default observer used is `LoggingStreamOffsetObserver` which simply 
logs when it is invoked.

#### Named Event Streaming

To start an event type stream, configure a `StreamProcessor` and run it:

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


#### Subscription Streaming

Subscription stream consumers work much like named event type streams:

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

- The `StreamConfiguration` is configured with an `subscriptionId`  instead of an `eventTypeName`.

- The inbuilt offset observer for a subscription stream will call Nakadi's checkpointing API to update the offset. You can replace this with your own implementation if you wish.

- A subscription stream also allows setting the `maxUncommittedEvents` as defined by the Nakadi API.

#### Backpressure and Buffering

A `StreamObserver` can ask for backpressure via the `requestBackPressure` 
method. This is be applied directly after each `onNext` call to the observer 
and so can be used to adjust backpressure dynamically. The client will 
make a best effort attempt to honor backpressure.

If the user wants events buffered into contiguous batches it can set a buffer 
size using `requestBuffer`. This is independent of the underlying HTTP 
stream - the stream will be consumed off the wire based on the API request 
settings and the batches buffered by the underlying processor. This is applied 
during setup and is fixed for the processor's lifecycle.

Users that don't care about backpresure controls can subclass the
 `StreamObserverBackPressure` class.


### Healthchecks

You can make healthcheck requests to the server:

```java
HealthCheckResource health = client.resources().health();
 
// check returning a response object, regardless of status
Response healthcheck = client.resources().health().healthcheck();
 
// ask to throw if the check failed (non 2xx code)
Response throwable = health.healthcheckThrowing();
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
  <artifactId>nakadi-java</artifactId>
  <version>0.0.0</version>
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
  compile 'net.dehora.nakadi:nakadi-java:0.0.0'
}  
```

### SBT

Add jcenter to `resolvers` in `build.sbt`:

```scala
resolvers += "jcenter" at "http://jcenter.bintray.com"
```

and add the project to `libraryDependencies` in `build.sbt`:

```scala
libraryDependencies += "net.dehora" % "nakadi-java" % "0.0.0"
```



## Idioms

### Fluent

The client prefers a fluent style, setters return `this` to allow chaining. 
Complex constructors use a builder pattern where needed. The JavaBeans 
get/set prefixing idiom is not used by the API, as is increasingly typical 
with modern Java code.

### Iterable pagination

Any API call that returns a collection, including ones that could be paginated expose Iterables contracts, allowing `forEach` or `iterator` access:

```java 
EventTypeCollection list = client.resources().eventTypes().list();
list.iterable().forEach(System.out::println);
 
Iterator<EventType> iterator = list.iterable().iterator();
while (iterator.hasNext()) {
  EventType next = iterator.next();
  System.out.println(next);
}
```

Pagination if it happens, is done automatically by the collection's backing iterable by following the `next` relation sent back by the server. 

You can if wish work with pages and hypertext links directly via the methods on `ResourceCollection` which each collection implements.


### HTTP Requests

Calls that result in HTTP requests are performed using resource classes. The 
results can be accessed as HTTP level responses or mapped to API objects.

You don't have to deal with HTTP responses from the API directly. If there 
is a failure then a `NakadiException` or a subclass will be thrown. The 
exception will have `Problem` information that can be examined. 

### Exceptions

Client exceptions are runtime exceptions by default. They extending from 
`NakadiException` which allows you to catch all errors under one type. The 
`NakadiException` embeds a `Problem` object which can be examined (Nakadi's 
API used problem json to describe errors). Local errors also contain 
Problem descriptions. 

The client will also throw an `IllegalArgumentException` in a number of places 
where null fields are not accepted or sensible as values, such as required 
parameters for builder classes. However the client performs no real data 
validation for API requests, leaving that to the server. These will cause an 
`InvalidException` to be thrown.

In a very few circumstances the API exposes a checked exception where it's 
neccessary the user handles the error; for example some exceptions from 
`StreamOffsetObserver` are checked.

## Build and Development

The project is built with [Gradle](http://gradle.org/) and uses the 
[Netflix Nebula](https://nebula-plugins.github.io/) plugins. The ./gradlew 
wrapper script will bootstrap the right Gradle version if it's not already 
installed. The main client jar file is build using the shadow plugin.

The main tasks are:

- `./gradlew build` : run a build and test
- `./gradlew clean` : clean down the build 
- `./gradlew clean shadow` : builds the client jar

## Internals

This section is not needed to use the client. It's here for the curious.

### Internal Dependencies 

The library has a small number of internal runtime dependencies. They are: 
 
  - Guava
  - Gson
  - OkHttp
  - RxJava
 
These dependencies have been selected because they have sane versioning models,
are robust, don't have global/classloader state, and have no transitive 
dependencies. They are all shaded - using the library does not require 
declaring them as dependencies. Shading has downsides notably making the 
published jar larger, and weird error conditions around globals/classloaders 
but this is considered preferable to dependency clashes, given the project's 
goals (also these libs avoid doing funky global/classloader things). The 
internal dependencies are considered private and may be revisited or removed 
at any time. (As a sidenote, the libraries were picked ahead of the client 
API design due to the goal of minimised dependencies.)

### Logging
 
The project uses the SLF4J API with information at debug, info, warn and error 
levels. If there's no binding on the classpath, you'll see a warning but the 
client will continue to work. 

SLF4J is the only dependency declared by the project, which breaks one of its 
goals (minimised depedencies) in favour of another (runtime observability). 
The project could (and an original prototype did) use java.util.Logging but this 
means every user has to configure it whether or not they use it to avoid noise 
in their application logs, and we observe that SLF4J seems to be in far wider 
use than java.util.Logging.

It's not something we're entirely comfortable with, and the dependency choice 
may be revisited in the future (eg where the client was considered sufficiently 
robust and well-instrumented that internal logging was not needed!).

### Business and Undefined Event Categories

These two categories have special serialization and deserialization ("serdes") 
handling. In the API definition they are both effectively raw JSON objects, 
which in Business case has a well known Nakadi defined field called `metadata`. 
Because of this they are problematic to represent as an defined API object as 
there's no fields to bind data to. This limits a client API to something like 
a String or a HashMap, or where depedencies and versions are not a concern, 
a parsed representation of the JSON based on whichever JSON lib the client 
happens to use.

You can "solve" this by just defining a generic for the entire business or 
undefined object but since part of the goal of this client is a complete 
implementation of what Nakadi defines there are two classes for these two 
categories, called `BusinessEventMapped` and `UndefinedEventMapped`. They 
work by shifting the custom part of the event to a field called `data`. When 
you create one these events and post it, all the information in the data 
field is lifted as direct children of the posted JSON object. That is the 
JSON doesn't contain a field called `data` that carries the custom information. 
When an event is marshalled all the fields that are direct children of the JSON
object are remapped into the classes `data` field, and for the business event 
the standard `metadata` field is marshalled into the classes' `EventMetdata` 
field.

By comparison, the `DataChangeEvent` doesn't have this issue, because the 
Nakadi API defines that category with a placeholder for custom data which 
gives a marshaller something to grab onto to. The `DataChangeEvent` class 
uses a generic type `T` which provides a much cleaner way to produce and 
consume custom data.

The pre-1.0.0 initial API uses a Map<String, Object> for the `data` field. It's 
very likely this will be superseded by a generic `T` field for 1.0.0 as that 
allows users to define the data using a local domain model instead of performing 
a second serdes step (it's a Java Map at the moment to verify the remapping 
idea was feasible to begin with). It also aligns with the `DataChangeEvent` 
class and provides a potential way forward to unify all three categories - 
currently they share a marker interface called `Event` but for practical 
purposes they are disjoint types.

### Stream Processing

Both the event and subscription based consumer API streams use a combination 
of RxJava and OkHttp. Which API is used depends on whether a subscription id 
or event type name is supplied to the `StreamProcessor`. Understanding how 
streams are consumed requires going into some details.

#### Batch Consumption

Batches are read using a `BufferedReader` from the underlying OkHttp reader 
and iterating over its `lines()` method tokenizes the stream into newline 
delimited chunks of JSON. Those chunks are marshalled to a 
`StreamBatchRecord<T>` where `T` represents the type of the event required by 
the `StreamObserver` implementation. For a subscription based stream, the 
`X-Nakadi-StreamId` that is send on the stream's opening header but not in 
each batch, is supplied in the `StreamCursorContext` for that batch along with 
the batch cursor. 

Empty keepalive batches use the same technique and are also sent along to the 
`StreamObserver`.

#### Thread Scheduling

When a `StreamProcessor` is started, it's placed into an executor and run 
in the background, away from the calling thread. Internally an RxJava 
observable is set up to consume the batch data coming from the HTTP response 
and the user supplied `StreamObserver` is registered to get callbacks via an 
RxJava Observer that is subscribed to the observer. 

The setup runs on two standard RxJava schedulers, io and computation. The 
io scheduler is used to consume the stream from the HTTP response, and 
the computation scheduler is where the user's `StreamObserver` runs. 

Because the supplied `StreamObserver` runs on the same computation scheduler
as the stream's batch processor, it blocks the underlying RxJava based batch 
processing but not the underlying io consumption layer (at least not until 
there's sufficient backpressure to cascade). This means that there is a small 
level of throughput decoupling from your batch processing and the stream coming
from the server. If upstream from your `StreamObserver`  is transiently slow, 
that doesn't immediately affect the HTTP connection. This may be useful in 
scenarios where your upstream event handler is falling behind or rate limiting 
you (eg Elasticsearch is throttling on batch inserts). 

#### Observer and Offset Observer Interaction

The `StreamOffsetObserver` supplied to the `StreamObserver` when it invoked 
blocks the `StreamObserver` - it is not called asynchronously, which in turn 
means it blocks the underlying computation scheduler - if 
`StreamOffsetObserver` slows down, it slows down overall batch processing. 
This is done to make it easier to reason about the consequences of a problem 
with a custom offset observer that is checkpointing the stream and reduce the 
likelihood of issues where offsets are processed incorrectly or out of order. 
If it fails, the `StreamObserver` can experience and handle that directly, eg 
by retrying with a backoff, or in extreme cases, shutting down the stream. An 
example of this is the default `StreamOffsetObserver` used for the Subscription 
based stream; it calls the Nakadi server to checkpoint progress and if it 
crashes it will throw an exception back directly back to the 
`StreamObserver`. 


#### Resumption

The stream has two levels of resumption - retries and restarts. 

The first, inner level is a basic retry with an exponential backoff that will 
increment up to a maximum delay. This is invoked for transient errors such as 
dropped connections or where the server is returning either 5xx level responses 
or retryable 4xx responses (such as a 429). The subscription based stream will 
also retry if it cannot obtain a partition to consume from, eg because all 
partitions have been taken by other clients - this allows some level of 
recovery such that a client will periodically check and compete for a partition 
to consume. When a retry occurs the `StreamObserver` `onStop` and `onStart` 
methods are called. Some errors are considered unretryable and will cause the 
stream to stop - one example is where the connection has been given cursors 
the server knows nothing about. 

The second, outer level, restarts the entire stream connection from scratch. 
This is invoked after the `StreamObserver`  `onCompleted` or `onError`  
methods are called. Clients that wish to stop the stream connection being 
restarted can call the `StreamProcessor` `stop` method. An exception to 
restart behaviour is when the stream is setup with a batch limit that 
indicates a bounded number of events are being asked for. Once that number 
of events is consumed the stream is closed and remains closed.

## Contributing

Please see the issue tracker for things to work on.

Before making a contribution, please let us know by posting a comment to the 
relevant issue. If you would like to propose a new feature, create a new issue 
first explaining the feature you’d like to contribute or bug you want to fix.

