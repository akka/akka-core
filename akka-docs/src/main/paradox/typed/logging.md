---
project.description: Logging options with Akka.
---
# Logging

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Logging](../logging.md).

## Dependency

To use Logging, you must at least use the Akka actors dependency in your project, and configure logging
via the SLF4J backend, such as Logback configuration.

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor-typed_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

[SLF4J](https://www.slf4j.org/) is used for logging and Akka provides access to an [org.slf4j.Logger](https://www.slf4j.org/api/org/slf4j/Logger.html) for a specific
actor via the @apidoc[typed.*.ActorContext]. You may also retrieve a `Logger` with the ordinary [org.slf4j.LoggerFactory](https://www.slf4j.org/api/org/slf4j/LoggerFactory.html).

To ensure that logging has minimal performance impact it's important that you configure an
asynchronous appender for the SLF4J backend. Logging generally means IO and locks,
which can slow down the operations of your code if it was performed synchronously.

## How to log

The @apidoc[typed.*.ActorContext] provides access to an [org.slf4j.Logger](https://www.slf4j.org/api/org/slf4j/Logger.html) for a specific actor.

Scala
:  @@snip [LoggingDocExamples.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/LoggingDocExamples.scala) { #context-log }

Java
:  @@snip [LoggingDocExamples.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/LoggingDocExamples.java) { #context-log }

The `Logger` via the `ActorContext` will automatically have a name that corresponds to the @apidoc[Behavior] of the
actor when the log is accessed the first time. The class name when using @apidoc[AbstractBehavior] or the class @scala[or object]
name where the `Behavior` is defined when using the functional style. You can set a custom logger name
with the @apidoc[setLoggerName](typed.*.ActorContext) {scala="#setLoggerName(name:String):Unit" java="#setLoggerName(java.lang.String)"} of the `ActorContext`.

Scala
:  @@snip [LoggingDocExamples.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/LoggingDocExamples.scala) { #logger-name }

Java
:  @@snip [LoggingDocExamples.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/LoggingDocExamples.java) { #logger-name }

The convention is to use logger names like fully qualified class names. The parameter to `setLoggerName`
can be a `String` or a `Class`, where the latter is convenience for the class name.

When logging via the `ActorContext` the path of the actor will automatically be added as `akkaSource`
Mapped Diagnostic Context (MDC) value. MDC is typically implemented with a @javadoc[ThreadLocal](java.lang.ThreadLocal) by the SLF4J backend.
To reduce performance impact, this MDC value is set when you access the @scala[@scaladoc[log](akka.actor.typed.scaladsl.ActorContext#log:org.slf4j.Logger)]@java[@javadoc[getLog()](akka.actor.typed.javadsl.ActorContext#getLog())] method so
you shouldn't cache the returned `Logger` in your own field. That is handled by `ActorContext` and retrieving
the `Logger` repeatedly with the @scala[`log`]@java[`getLog`] method has low overhead.
The MDC is cleared automatically after processing of current message has finished.

@@@ note

The `Logger` is thread-safe but the @scala[`log`]@java[`getLog`] method in `ActorContext` is not
thread-safe and should not be accessed from threads other than the ordinary actor message processing
thread, such as @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] callbacks.

@@@

It's also perfectly fine to use a [Logger](https://www.slf4j.org/api/org/slf4j/Logger.html) retrieved via [org.slf4j.LoggerFactory](https://www.slf4j.org/api/org/slf4j/LoggerFactory.html), but then the logging
events will not include the `akkaSource` MDC value. This is the recommended way when logging outside
of an actor, including logging from @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] callbacks.

Scala
:  @@snip [LoggingDocExamples.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/LoggingDocExamples.scala) { #logger-factory }

Java
:  @@snip [LoggingDocExamples.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/LoggingDocExamples.java) { #logger-factory }

### Placeholder arguments

The log message may contain argument placeholders `{}`, which will be substituted if the log level is enabled.
Compared to constructing a full string for the log message this has the advantage of avoiding superfluous
string concatenation and object allocations when the log level is disabled. Some logging backends
may also use these message templates before argument substitution to group and filter logging events.

It can be good to know that 3 or more arguments will result in the relatively small cost of allocating
an array (vararg parameter) also when the log level is disabled. The methods with 1 or 2 arguments
don't allocate the vararg array.

### Behaviors.logMessages

If you want very detailed logging of messages and signals you can decorate a @apidoc[Behavior]
with @apidoc[Behaviors.logMessages](Behaviors$) {scala="#logMessages[T](logOptions:akka.actor.typed.LogOptions,behavior:akka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#logMessages(akka.actor.typed.LogOptions,akka.actor.typed.Behavior)"}.

Scala
:  @@snip [LoggingDocExamples.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/LoggingDocExamples.scala) { #logMessages }

Java
:  @@snip [LoggingDocExamples.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/LoggingDocExamples.java) { #logMessages }


## MDC

[MDC](https://logback.qos.ch/manual/mdc.html) allows for adding additional context dependent attributes to log entries.
Out of the box, Akka will place the path of the actor in the the MDC attribute `akkaSource`.

One or more tags can also be added to the MDC using the @apidoc[ActorTags$] props. The tags will be rendered as a comma separated
list and be put in the MDC attribute `akkaTags`. This can be used to categorize log entries from a set of different actors
to allow easier filtering of logs:

Scala
:  @@snip [LoggingDocExamples.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/LoggingDocExamples.scala) { #tags }

Java
:  @@snip [LoggingDocExamples.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/LoggingDocExamples.java) { #tags }

In addition to these two built in MDC attributes you can also decorate a @apidoc[Behavior] with @apidoc[Behaviors.withMdc](Behaviors$) {scala="#withMdc[T](staticMdc:Map[String,String],mdcForMessage:T=%3EMap[String,String])(behavior:akka.actor.typed.Behavior[T])(implicitevidence$4:scala.reflect.ClassTag[T]):akka.actor.typed.Behavior[T]" java="#withMdc(java.lang.Class,java.util.Map,akka.japi.function.Function,akka.actor.typed.Behavior)"} or 
use the [org.slf4j.MDC](https://www.slf4j.org/api/org/slf4j/MDC.html) API directly.

The `Behaviors.withMdc` decorator has two parts. A static `Map` of MDC attributes that are not changed,
and a dynamic `Map` that can be constructed for each message.

Scala
:  @@snip [LoggingDocExamples.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/LoggingDocExamples.scala) { #withMdc }

Java
:  @@snip [LoggingDocExamples.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/LoggingDocExamples.java) { #withMdc }

If you use the MDC API directly, be aware that MDC is typically implemented with a @javadoc[ThreadLocal](java.lang.ThreadLocal) by the SLF4J backend.
Akka clears the MDC if logging is performed via the @scala[@scaladoc[log](akka.actor.typed.scaladsl.ActorContext#log:org.slf4j.Logger)]@java[@javadoc[getLog()](akka.actor.typed.javadsl.ActorContext#getLog())] of the `ActorContext` and it is cleared
automatically after processing of current message has finished, but only if you accessed @scala[`log`]@java[`getLog()`].
The entire MDC is cleared, including attributes that you add yourself to the MDC.
MDC is not cleared automatically if you use a [Logger](https://www.slf4j.org/api/org/slf4j/Logger.html) via [LoggerFactory](https://www.slf4j.org/api/org/slf4j/LoggerFactory.html) or not touch @scala[`log`]@java[`getLog()`]
in the `ActorContext`.

## SLF4J API compatibility

Since Akka 2.10.0, only SLF4j version `2.0` is supported. 

It is not possible to mix a logger backend supporting one version with SLF4J API of older versions.

## SLF4J backend

To ensure that logging has minimal performance impact it's important that you configure an
asynchronous appender for the SLF4J backend. Logging generally means IO and locks,
which can slow down the operations of your code if it was performed synchronously.

@@@ warning

For production the SLF4J backend should be configured with an asynchronous appender as described here.
Otherwise, there is a risk of reduced performance and thread starvation problems of the dispatchers
that are running actors and other tasks.

@@@

### Logback

`akka-actor-typed` includes a dependency to the `slf4j-api`. In your runtime, you also need a SLF4J backend.
We recommend [Logback](https://logback.qos.ch/):

@@dependency[sbt,Maven,Gradle] {
  group="ch.qos.logback"
  artifact="logback-classic"
  version="$logback_version$"
}

Logback has flexible configuration options and details can be found in the
[Logback manual](https://logback.qos.ch/manual/configuration.html) and other external resources.

One part that is important to highlight is the importance of configuring an [AsyncAppender](https://logback.qos.ch/manual/appenders.html#AsyncAppender),
because it offloads rendering of logging events to a background thread, increasing performance. It doesn't block
the threads of the @apidoc[typed.ActorSystem] while the underlying infrastructure writes the log messages to disk or other configured
destination. It also contains a feature which will drop `INFO` and `DEBUG` messages if the logging
load is high.

A starting point for configuration of `logback.xml` for production:

@@snip [logback.xml](/akka-actor-typed-tests/src/test/resources/logback-doc-prod.xml)

Note that the [AsyncAppender](https://logback.qos.ch/apidocs/ch.qos.logback.classic/ch/qos/logback/classic/AsyncAppender.html) may drop log events if the queue becomes full, which may happen if the
logging backend can't keep up with the throughput of produced log events. Dropping log events is necessary
if you want to gracefully degrade your application if only your logging backend or filesystem is experiencing
issues. 

An alternative of the Logback `AsyncAppender` with better performance is the [Logstash async appender](https://github.com/logstash/logstash-logback-encoder#async-appenders).

The ELK-stack is commonly used as logging infrastructure for production:

* [Logstash Logback encoder](https://github.com/logstash/logstash-logback-encoder)
* [Logstash](https://www.elastic.co/guide/en/logstash/current/index.html)
* [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
* [Kibana](https://www.elastic.co/guide/en/kibana/current/index.html)

For development you might want to log to standard out, but also have all debug level logging to file, like
in this example:

@@snip [logback.xml](/akka-actor-typed-tests/src/test/resources/logback-doc-dev.xml)

Place the `logback.xml` file in `src/main/resources/logback.xml`. For tests you can define different
logging configuration in `src/test/resources/logback-test.xml`.

#### MDC values

When logging via the  @scala[@scaladoc[log](akka.actor.typed.scaladsl.ActorContext#log:org.slf4j.Logger)]@java[@javadoc[getLog()](akka.actor.typed.javadsl.ActorContext#getLog())] of the `ActorContext`, as described in
@ref:[How to log](#how-to-log), Akka includes a few MDC properties:

* `akkaSource`: the actor's path
* `akkaAddress`: the full address of the ActorSystem, including hostname and port if Cluster is enabled
* `akkaTags`: tags defined in the @apidoc[typed.Props] of the actor
* `sourceActorSystem`: the name of the ActorSystem

These MDC properties can be included in the Logback output with for example `%X{akkaSource}` specifier within the
[pattern layout configuration](https://logback.qos.ch/manual/layouts.html#mdc):

```
  <encoder>
    <pattern>%date{ISO8601} %-5level %logger{36} %X{akkaSource} - %msg%n</pattern>
  </encoder>
```

All MDC properties as key-value entries can be included with `%mdc`:

```
  <encoder>
    <pattern>%date{ISO8601} %-5level %logger{36} - %msg MDC: {%mdc}%n</pattern>
  </encoder>
```


## Internal logging by Akka

### Event bus

For historical reasons logging by the Akka internals and by classic actors are performed asynchronously
through an event bus. Such log events are processed by an event handler actor, which then emits them to
SLF4J or directly to standard out.

When `akka-actor-typed` and `akka-slf4j` are on the classpath this event handler actor will emit the events to SLF4J.
The @apidoc[akka.event.slf4j.Slf4jLogger](Slf4jLogger) and @apidoc[akka.event.slf4j.Slf4jLoggingFilter](Slf4jLoggingFilter) are enabled automatically
without additional configuration. This can be disabled by `akka.use-slf4j=off` configuration property.

In other words, you don't have to do anything for the Akka internal logging to end up in your configured
SLF4J backend.

### Log level

Ultimately the log level defined in the SLF4J backend is used. For the Akka internal logging it will
also check the level defined by the SLF4J backend before constructing the final log message and
emitting it to the event bus.

However, there is an additional `akka.loglevel` configuration property that defines if logging events
with lower log level should be discarded immediately without consulting the SLF4J backend. By default
this is at `INFO` level, which means that `DEBUG` level logging from the Akka internals will not
reach the SLF4J backend even if `DEBUG` is enabled in the backend.

You can enable `DEBUG` level for `akka.loglevel` and control the actual level in the SLF4j backend
without any significant overhead, also for production.

```
akka.loglevel = "DEBUG"
```

To turn off all Akka internal logging (not recommended) you can configure the log levels to be
`OFF` like this.

```
akka {
  stdout-loglevel = "OFF"
  loglevel = "OFF"
}
```

The `stdout-loglevel` is only in effect during system startup and shutdown, and setting
it to `OFF` as well, ensures that nothing gets logged during system startup or shutdown.

See @ref:[Logger names](#logger-names) for configuration of log level in SLF4J backend for certain
modules of Akka.

### Logging to stdout during startup and shutdown

When the actor system is starting up and shutting down the configured `loggers` are not used.
Instead log messages are printed to stdout (System.out). The default log level for this
stdout logger is `WARNING` and it can be silenced completely by setting
`akka.stdout-loglevel=OFF`.

### Logging of Dead Letters

By default messages sent to dead letters are logged at info level. Existence of dead letters
does not necessarily indicate a problem, but they are logged by default for the sake of caution.
After a few messages this logging is turned off, to avoid flooding the logs.
You can disable this logging completely or adjust how many dead letters are
logged. During system shutdown it is likely that you see dead letters, since pending
messages in the actor mailboxes are sent to dead letters. You can also disable logging
of dead letters during shutdown.

```
akka {
  log-dead-letters = 10
  log-dead-letters-during-shutdown = on
}
```

To customize the logging further or take other actions for dead letters you can subscribe
to the @ref:[Event Stream](event-stream.md).

### Auxiliary logging options

Akka has a few configuration options for very low level debugging. These make more sense in development than in production.

You almost definitely need to have logging set to DEBUG to use any of the options below:

```
akka {
  loglevel = "DEBUG"
}
```

This config option is very good if you want to know what config settings are loaded by Akka:

```
akka {
  # Log the complete configuration at INFO level when the actor system is started.
  # This is useful when you are uncertain of what configuration is used.
  log-config-on-start = on
}
```

If you want unhandled messages logged at DEBUG:

```
akka {
  actor {
    debug {
      # enable DEBUG logging of unhandled messages
      unhandled = on
    }
  }
}
```

If you want to monitor subscriptions (subscribe/unsubscribe) on the ActorSystem.eventStream:

```
akka {
  actor {
    debug {
      # enable DEBUG logging of subscription changes on the eventStream
      event-stream = on
    }
  }
}
```

<a id="logging-remote"></a>
### Auxiliary remote logging options

If you want to see all messages that are sent through remoting at DEBUG log level, use the following config option.
Note that this logs the messages as they are sent by the transport layer, not by an actor.

```
akka.remote.artery {
  # If this is "on", Akka will log all outbound messages at DEBUG level,
  # if off then they are not logged
  log-sent-messages = on
}
```

If you want to see all messages that are received through remoting at DEBUG log level, use the following config option.
Note that this logs the messages as they are received by the transport layer, not by an actor.

```
akka.remote.artery {
  # If this is "on", Akka will log all inbound messages at DEBUG level,
  # if off then they are not logged
  log-received-messages = on
}
```

Logging of message types with payload size in bytes larger than the configured `log-frame-size-exceeding`.

```
akka.remote.artery {
  log-frame-size-exceeding = 10000b
}
```

### MDC values from Akka internal logging

Since the logging is done asynchronously, the thread in which the logging was performed is captured in
MDC with attribute name `sourceThread`.

The path of the actor in which the logging was performed is available in the MDC with attribute name `akkaSource`.

The actor system name in which the logging was performed is available in the MDC with attribute name `sourceActorSystem`,
but that is typically also included in the `akkaSource` attribute.

The address of the actor system, containing host and port if the system is using cluster, is available through `akkaAddress`.

For typed actors the log event timestamp is taken when the log call was made but for
Akka's _internal_ logging as well as the classic actor logging is asynchronous which means that the timestamp of a log entry is taken from
when the underlying logger implementation is called, which can be surprising at first.
If you want to more accurately output the timestamp for such loggers, use the MDC attribute `akkaTimestamp`. Note that 
the MDC key will not have any value for a typed actor.

### Markers

Akka is logging some events with markers. Some of these events also include structured MDC properties. 

* The "SECURITY" marker is used for highlighting security related events or incidents.
* Akka Actor is using the markers defined in @apidoc[akka.actor.ActorLogMarker$].
* Akka Cluster is using the markers defined in @apidoc[akka.cluster.ClusterLogMarker$].
* Akka Remoting is using the markers defined in @apidoc[akka.remote.RemoteLogMarker$].
* Akka Cluster Sharding is using the markers defined in @apidoc[akka.cluster.sharding.ShardingLogMarker$].

Markers and MDC properties are automatically picked up by the [Logstash Logback encoder](https://github.com/logstash/logstash-logback-encoder).

The marker can be included in the Logback output with `%marker` and all MDC properties as key-value entries with `%mdc`.

```
  <encoder>
    <pattern>[%date{ISO8601}] [%level] [%logger] [%marker] [%thread] - %msg MDC: {%mdc}%n</pattern>
  </encoder>
```

### Logger names

It can be useful to enable debug level or other SLF4J backend configuration for certain modules of Akka when
troubleshooting. Those logger names are typically prefixed with the package name of the classes in that module.
For example, in Logback the configuration may look like this to enable debug logging for Cluster Sharding: 

```
   <logger name="akka.cluster.sharding" level="DEBUG" />

    <root level="INFO">
        <appender-ref ref="ASYNC"/>
    </root>
```

Other examples of logger names or prefixes:

```
akka.cluster
akka.cluster.Cluster
akka.cluster.ClusterHeartbeat
akka.cluster.ClusterGossip
akka.cluster.ddata
akka.cluster.pubsub
akka.cluster.singleton
akka.cluster.sharding
akka.coordination.lease
akka.discovery
akka.persistence
akka.remote
```

## Logging in tests

Testing utilities are described in @ref:[Testing](testing-async.md#test-of-logging).
