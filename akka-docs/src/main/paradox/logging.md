# Classic Logging

@@include[includes.md](includes.md) { #actor-api }
For the new API see @ref[Logging](typed/logging.md).

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Logging, you must at least use the Akka actors dependency in your project, and will most likely want to configure logging via the SLF4J module (@ref:[see below](#slf4j)).

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

@@project-info{ projectId="akka-slf4j" }

## Introduction

Logging in Akka is not tied to a specific logging backend. By default
log messages are printed to STDOUT, but you can plug-in a SLF4J logger or
your own logger. Logging is performed asynchronously to ensure that logging
has minimal performance impact. Logging generally means IO and locks,
which can slow down the operations of your code if it was performed
synchronously.

## How to Log

Create a @apidoc[LoggingAdapter] and use the `error`, `warning`, `info`, or `debug` methods,
as illustrated in this example:

Scala
:   @@snip [LoggingDocSpec.scala](/akka-docs/src/test/scala/docs/event/LoggingDocSpec.scala) { #my-actor }

Java
:   @@snip [LoggingDocTest.java](/akka-docs/src/test/java/jdocs/event/LoggingDocTest.java) { #imports }
    @@snip [LoggingDocTest.java](/akka-docs/src/test/java/jdocs/event/LoggingDocTest.java) { #my-actor }

@@@ div { .group-scala }


For convenience, you can mix in the `log` member into actors, instead of defining it as above.

```scala
class MyActor extends Actor with akka.actor.ActorLogging {
  ...
}
```

@@@

The first parameter to @scala[@scaladoc[Logging](akka.event.Logging$#apply[T](bus:akka.event.LoggingBus,logSource:T)(implicitevidence$5:akka.event.LogSource[T]):akka.event.LoggingAdapter)] @java[@javadoc[Logging.getLogger](akka.event.Logging#getLogger(akka.event.LoggingBus,java.lang.Object))] could also be any
@apidoc[LoggingBus], specifically @scala[@scaladoc[system.eventStream](akka.actor.ActorSystem#eventStream:akka.event.EventStream)] @java[@javadoc[system.getEventStream()](akka.actor.ActorSystem#getEventStream())]. 
In the demonstrated case, the actor system's address is included in the `akkaSource`
representation of the log source (see @ref:[Logging Thread, Akka Source and Actor System in MDC](#logging-thread-akka-source-and-actor-system-in-mdc)),
while in the second case this is not automatically done.
The second parameter to @scala[`Logging`] @java[`Logging.getLogger`] is the source of this logging channel.
The source object is translated to a String according to the following rules:

 * if it is an Actor or ActorRef, its path is used
 * in case of a String it is used as is
 * in case of a Class an approximation of its `simpleName` is used
 * in all other cases @scala[a compile error occurs unless an implicit
@scaladoc[LogSource[T]](akka.event.LogSource) is in scope for the type in question] @java[the `simpleName` of its class] is used

The log message may contain argument placeholders `{}`, which will be
substituted if the log level is enabled. Compared to constructing a full string
for the log message this has the advantage of avoiding superfluous string concatenation
and object allocations when the log level is disabled. Giving more arguments than
placeholders results in a warning being appended to the log statement (i.e. on
the same line with the same severity). You may pass an array as the only
substitution argument to have its elements be treated individually:

Scala
:   @@snip [LoggingDocSpec.scala](/akka-docs/src/test/scala/docs/event/LoggingDocSpec.scala) { #array }

Java
:   @@snip [LoggingDocTest.java](/akka-docs/src/test/java/jdocs/event/LoggingDocTest.java) { #array }

The Java @javadoc[Class](java.lang.Class) of the log source is also included in the generated
@apidoc[akka.event.Logging.LogEvent]. In case of a simple string this is replaced with a “marker”
class @apidoc[akka.event.DummyClassForStringSources] in order to allow special
treatment of this case, e.g. in the SLF4J event listener which will then use
the string instead of the class’ name for looking up the logger instance to
use.

### Logging of Dead Letters

By default messages sent to dead letters are logged at `INFO` level. Existence of dead letters
does not necessarily indicate a problem, but they are logged by default for the sake of caution.
After a few messages this logging is turned off, to avoid flooding the logs.
You can disable this logging completely or adjust how many dead letters are
logged. During system shutdown it is likely that you see dead letters, since pending
messages in the actor mailboxes are sent to dead letters. You can also disable logging
of dead letters during shutdown.

```ruby
akka {
  log-dead-letters = 10
  log-dead-letters-during-shutdown = on
}
```

To customize the logging further or take other actions for dead letters you can subscribe
to the @ref:[Event Stream](event-bus.md#event-stream).

### Auxiliary logging options

Akka has a few configuration options for very low level debugging. These make more sense in development than in production.

You almost definitely need to have logging set to `DEBUG` to use any of the options below:

```ruby
akka {
  loglevel = "DEBUG"
}
```

This config option is very good if you want to know what config settings are loaded by Akka:

```ruby
akka {
  # Log the complete configuration at INFO level when the actor system is started.
  # This is useful when you are uncertain of what configuration is used.
  log-config-on-start = on
}
```

@@@ div { .group-scala }

If you want very detailed logging of user-level messages then wrap your actors' behaviors with
@scaladoc[LoggingReceive](akka.event.LoggingReceive) and enable the `receive` option:

```ruby
akka {
  actor {
    debug {
      # enable function of LoggingReceive, which is to log any received message at
      # DEBUG level
      receive = on
    }
  }
}
```

@@@

If you want very detailed logging of all automatically received messages that are processed
by Actors:

```ruby
akka {
  actor {
    debug {
      # enable DEBUG logging of all AutoReceiveMessages (Kill, PoisonPill etc.)
      autoreceive = on
    }
  }
}
```

If you want very detailed logging of all lifecycle changes of Actors (restarts, deaths etc.):

```ruby
akka {
  actor {
    debug {
      # enable DEBUG logging of actor lifecycle changes
      lifecycle = on
    }
  }
}
```

If you want unhandled messages logged at `DEBUG`:

```ruby
akka {
  actor {
    debug {
      # enable DEBUG logging of unhandled messages
      unhandled = on
    }
  }
}
```

If you want very detailed logging of all events, transitions and timers of FSM Actors that extend LoggingFSM:

```ruby
akka {
  actor {
    debug {
      # enable DEBUG logging of all LoggingFSMs for events, transitions and timers
      fsm = on
    }
  }
}
```

If you want to monitor subscriptions (subscribe/unsubscribe) on the ActorSystem.eventStream:

```ruby
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

If you want to see all messages that are sent through remoting at `DEBUG` log level, use the following config option. Note that this logs the messages as they are sent by the transport layer, not by an actor.

```ruby
akka.remote.artery {
  # If this is "on", Akka will log all outbound messages at DEBUG level,
  # if off then they are not logged
  log-sent-messages = on
}
```

If you want to see all messages that are received through remoting at `DEBUG` log level, use the following config option. Note that this logs the messages as they are received by the transport layer, not by an actor.

```ruby
akka.remote.artery {
  # If this is "on", Akka will log all inbound messages at DEBUG level,
  # if off then they are not logged
  log-received-messages = on
}
```

Also see the @ref:[logging options for TestKit](testing.md#actor-logging).

@@@ div { .group-scala }

### Translating Log Source to String and Class

The rules for translating the source object to the source string and class
which are inserted into the @scaladoc[LogEvent](akka.event.Logging.LogEvent) during runtime are implemented
using implicit parameters and thus fully customizable: create your own
instance of @scaladoc[LogSource[T]](akka.event.LogSource) and have it in scope when creating the
logger.

@@snip [LoggingDocSpec.scala](/akka-docs/src/test/scala/docs/event/LoggingDocSpec.scala) { #my-source }

This example creates a log source which mimics traditional usage of Java
loggers, which are based upon the originating object’s class name as log
category. The override of @scaladoc[getClazz](akka.event.LogSource#getClazz(t:T):Class[_]) is only included for demonstration
purposes as it contains exactly the default behavior.

@@@

@@@ note { .group-scala }

You may also create the string representation up front and pass that in as
the log source, but be aware that then the `Class[_]` which will be
put in the @scaladoc[LogEvent](akka.event.Logging.LogEvent) is
@scaladoc[akka.event.DummyClassForStringSources](akka.event.DummyClassForStringSources).

The SLF4J event listener treats this case specially (using the actual string
to look up the logger instance to use instead of the class’ name), and you
might want to do this also in case you implement your own logging adapter.

@@@


### Turn Off Logging

To turn off logging you can configure the log levels to be `OFF` like this.

```ruby
akka {
  stdout-loglevel = "OFF"
  loglevel = "OFF"
}
```

The `stdout-loglevel` is only in effect during system startup and shutdown, and setting
it to `OFF` as well, ensures that nothing gets logged during system startup or shutdown.

## Loggers

Logging is performed asynchronously through an event bus. Log events are processed by an event handler actor
that receives the log events in the same order they were emitted.

@@@ note

The event handler actor does not have a bounded inbox and is run on the default dispatcher. This means
that logging extreme amounts of data may affect your application badly. This can be somewhat mitigated by
using an async logging backend though. (See @ref:[Using the SLF4J API directly](#slf4j-directly))

@@@

You can configure which event handlers are created at system start-up and listen to logging events. That is done using the
`loggers` element in the @ref:[configuration](general/configuration.md).
Here you can also define the log level. More fine grained filtering based on the log source
can be implemented in a custom @apidoc[LoggingFilter], which can be defined in the `logging-filter`
configuration property.

```ruby
akka {
  # Loggers to register at boot time (akka.event.Logging$DefaultLogger logs
  # to STDOUT)
  loggers = ["akka.event.Logging$DefaultLogger"]
  # Options: OFF, ERROR, WARNING, INFO, DEBUG
  loglevel = "DEBUG"
}
```

The default one logs to STDOUT and is registered by default. It is not intended
to be used for production. There is also an @ref:[SLF4J](#slf4j)
logger available in the 'akka-slf4j' module.

@@@ note

If `akka-actor-typed` is available on your classpath, logging will automatically switch to @ref:[SLF4J](#slf4j) instead of 
the default logger. See the  @ref:[Akka typed logging](typed/logging.md#event-bus) docs for more details.

@@@

Example of creating a listener:

Scala
:   @@snip [LoggingDocSpec.scala](/akka-docs/src/test/scala/docs/event/LoggingDocSpec.scala) { #my-event-listener }

Java
:   @@snip [LoggingDocTest.java](/akka-docs/src/test/java/jdocs/event/LoggingDocTest.java) { #imports #imports-listener }
    @@snip [LoggingDocTest.java](/akka-docs/src/test/java/jdocs/event/LoggingDocTest.java) { #my-event-listener }

## Logging to stdout during startup and shutdown

When the actor system is starting up and shutting down the configured `loggers` are not used.
Instead log messages are printed to stdout (System.out). The default log level for this
stdout logger is `WARNING` and it can be silenced completely by setting
`akka.stdout-loglevel=OFF`.

## SLF4J

Akka provides a logger for [SLF4J](https://www.slf4j.org/). This module is available in the 'akka-slf4j.jar'.
It has a single dependency: the slf4j-api jar. In your runtime, you also need a SLF4J backend. We recommend [Logback](https://logback.qos.ch/):

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-slf4j_$scala.binary.version$"
  version=AkkaVersion
  group2="ch.qos.logback"
  artifact2="logback-classic"
  version2="$logback_version$"
}

You need to enable the Slf4jLogger in the `loggers` element in
the @ref:[configuration](general/configuration.md). Here you can also define the log level of the event bus.
More fine grained log levels can be defined in the configuration of the SLF4J backend
(e.g. logback.xml). You should also define @apidoc[akka.event.slf4j.Slf4jLoggingFilter] in
the `logging-filter` configuration property. It will filter the log events using the backend
configuration (e.g. logback.xml) before they are published to the event bus.

@@@ warning

If you set the `loglevel` to a higher level than `DEBUG`, any `DEBUG` events will be filtered
out already at the source and will never reach the logging backend, regardless of how the backend
is configured.

You can enable `DEBUG` level for `akka.loglevel` and control the actual level in the SLF4J backend
without any significant overhead, also for production.

@@@

```ruby
akka {
  loggers = ["akka.event.slf4j.Slf4jLogger"]
  loglevel = "DEBUG"
  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
}
```

One gotcha is that the timestamp is attributed in the event handler, not when actually doing the logging.

The SLF4J logger selected for each log event is chosen based on the
@scala[`Class[_]`] @java[`Class`] of the log source specified when creating the
@apidoc[LoggingAdapter], unless that was given directly as a string in which
case that string is used (i.e. @scala[@javadoc[LoggerFactory.getLogger(c: Class[_ ])](org.slf4j.LoggerFactory#getLogger(java.lang.Class))] @java[@javadoc[LoggerFactory.getLogger(Class c)](org.slf4j.LoggerFactory#getLogger(java.lang.Class))] is used in
the first case and @scala[@javadoc[LoggerFactory.getLogger(s: String)](org.slf4j.LoggerFactory#getLogger(java.lang.String))] @java[@javadoc[LoggerFactory.getLogger(String s)](org.slf4j.LoggerFactory#getLogger(java.lang.String))] in the second).

@@@ note

Beware that the actor system’s name is appended to a @javadoc[String](java.lang.String) log
source if the LoggingAdapter was created giving an @apidoc[akka.actor.ActorSystem] to
the factory. If this is not intended, give a @apidoc[LoggingBus] instead as
shown below:

@@@

Scala
:   ```scala
val log = Logging(system.eventStream, "my.nice.string")
```

Java
:   ```java
final LoggingAdapter log = Logging.getLogger(system.eventStream(), "my.string");
```

<a id="slf4j-directly"></a>
### Using the SLF4J API directly

If you use the SLF4J API directly in your application, remember that the logging operations will block
while the underlying infrastructure writes the log statements.

This can be avoided by configuring the logging implementation to use
a non-blocking appender. Logback provides [AsyncAppender](https://logback.qos.ch/manual/appenders.html#AsyncAppender)
that does this.

### Logback configuration

Logback has flexible configuration options and details can be found in the
[Logback manual](https://logback.qos.ch/manual/configuration.html) and other external resources.

One part that is important to highlight is the importance of configuring an [AsyncAppender](https://logback.qos.ch/manual/appenders.html#AsyncAppender),
because it offloads rendering of logging events to a background thread, increasing performance. It doesn't block
the threads of the @apidoc[akka.actor.ActorSystem] while the underlying infrastructure writes the log messages to disk or other configured
destination. It also contains a feature which will drop `INFO` and `DEBUG` messages if the logging
load is high.

A starting point for configuration of `logback.xml` for production:

@@snip [logback.xml](/akka-actor-typed-tests/src/test/resources/logback-doc-prod.xml)

For development you might want to log to standard out, but also have all `DEBUG` level logging to file, like
in this example:

@@snip [logback.xml](/akka-actor-typed-tests/src/test/resources/logback-doc-dev.xml)

Place the `logback.xml` file in `src/main/resources/logback.xml`. For tests you can define different
logging configuration in `src/test/resources/logback-test.xml`.

MDC properties can be included in the Logback output with for example `%X{akkaSource}` specifier within the
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

### Logging Thread, Akka Source and Actor System in MDC

Since the logging is done asynchronously the thread in which the logging was performed is captured in
Mapped Diagnostic Context (MDC) with attribute name `sourceThread`.

@@@ note

It will probably be a good idea to use the `sourceThread` MDC value also in
non-Akka parts of the application in order to have this property consistently
available in the logs.

@@@

Another helpful facility is that Akka captures the actor’s address when
instantiating a logger within it, meaning that the full instance identification
is available for associating log messages e.g. with members of a router. This
information is available in the MDC with attribute name `akkaSource`.

The address of the actor system, containing host and port if the system is using cluster, is available through `akkaAddress`.

Finally, the actor system in which the logging was performed
is available in the MDC with attribute name `sourceActorSystem`.

For more details on what this attribute contains—also for non-actors—please see
[How to Log](#how-to-log).

### More accurate timestamps for log output in MDC

Akka's logging is asynchronous which means that the timestamp of a log entry is taken from
when the underlying logger implementation is called, which can be surprising at first.
If you want to more accurately output the timestamp, use the MDC attribute `akkaTimestamp`.

### MDC values defined by the application

One useful feature available in Slf4j is [MDC](https://logback.qos.ch/manual/mdc.html),
Akka has a way to let the application specify custom values, for this you need to use a
specialized @apidoc[LoggingAdapter], the @apidoc[DiagnosticLoggingAdapter]. In order to
get it you can use the factory, providing an @scala[@scaladoc[Actor](akka.actor.Actor)] @java[@javadoc[AbstractActor](akka.actor.AbstractActor)] as logSource:

Scala
:   ```scala
// Within your Actor
val log: DiagnosticLoggingAdapter = Logging(this);
```

Java
:   ```java
// Within your AbstractActor
final DiagnosticLoggingAdapter log = Logging.getLogger(this);
```

Once you have the logger, you need to add the custom values before you log something.
This way, the values will be put in the SLF4J MDC right before appending the log and removed after.

@@@ note

The cleanup (removal) should be done in the actor at the end,
otherwise, next message will log with same MDC values,
if it is not set to a new map. Use @apidoc[log.clearMDC()](DiagnosticLoggingAdapter) {scala="#clearMDC():Unit" java="#clearMDC()"}.

@@@

Scala
:   @@snip [LoggingDocSpec.scala](/akka-docs/src/test/scala/docs/event/LoggingDocSpec.scala) { #mdc }

Java
:   @@snip [LoggingDocTest.java](/akka-docs/src/test/java/jdocs/event/LoggingDocTest.java) { #imports-mdc }
    @@snip [LoggingDocTest.java](/akka-docs/src/test/java/jdocs/event/LoggingDocTest.java) { #mdc-actor }

@@@ div { .group-scala }

For convenience, you can mix in the `log` member into actors, instead of defining it as above.
This trait also lets you override @scaladoc[mdc(msg: Any)](akka.actor.DiagnosticActorLogging#mdc(currentMessage:Any):akka.event.Logging.MDC) for specifying MDC values
depending on current message and lets you forget about the cleanup as well, since it already does it for you.

@@snip [LoggingDocSpec.scala](/akka-docs/src/test/scala/docs/event/LoggingDocSpec.scala) { #mdc-actor }

@@@

Now, the values will be available in the MDC, so you can use them in the layout pattern:

```
<appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
  <encoder>
    <pattern>
      %-5level %logger{36} [req: %X{requestId}, visitor: %X{visitorId}] - %msg%n
    </pattern>
  </encoder>
</appender>
```

All MDC properties as key-value entries can be included with `%mdc`:

```
  <encoder>
    <pattern>%date{ISO8601} %-5level %logger{36} - %msg MDC: {%mdc}%n</pattern>
  </encoder>
```

### Using Markers

Some logging libraries allow, in addition to MDC data, attaching so called "markers" to log statements.
These are used to filter out rare and special events, for example you might want to mark logs that detect
some malicious activity and mark them with a `SECURITY` tag, and in your appender configuration make these
trigger emails and other notifications immediately.

Markers are available through the LoggingAdapters, when obtained via @apidoc[Logging.withMarker](akka.event.Logging$) {scala="#withMarker(logSource:akka.actor.Actor):akka.event.DiagnosticMarkerBusLoggingAdapter" java="#withMarker(akka.actor.Actor)"}.
The first argument passed into all log calls then should be a @apidpc[akka.event.LogMarker].

The slf4j bridge provided by Akka in `akka-slf4j` will automatically pick up this marker value and make it available to SLF4J.

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

#### Using SLF4J's Markers

It is also possible to use the @javadoc[org.slf4j.Marker](org.slf4j.Marker) with the @apidoc[LoggingAdapter] when using slf4j.

Since the akka-actor library avoids depending on any specific logging library, the support for this is included in `akka-slf4j`,
which provides the @apidoc[Slf4jLogMarker] type which can be passed in as first argument instead of the logging framework agnostic LogMarker
type from `akka-actor`. The most notable difference between the two is that slf4j's Markers can have child markers, so one can
rely more information using them rather than just a single string.
