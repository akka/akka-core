# Reactive Streams Interop

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Streams, add the module to your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-stream_$scala.binary.version$"
  version=AkkaVersion
}

<a id="reactive-streams-integration"></a>
## Overview

Akka Streams implements the [Reactive Streams](https://www.reactive-streams.org/) standard for asynchronous stream processing with non-blocking
back pressure. 

Since Java 9 the APIs of Reactive Streams has been included in the Java Standard library, under the  `java.util.concurrent.Flow` 
namespace. For Java 8 there is instead a separate Reactive Streams artifact with the same APIs in the package `org.reactivestreams`.

Akka streams provides interoperability for both these two API versions, the Reactive Streams interfaces directly through factories on the
regular `Source` and `Sink` APIs. For the Java 9 and later built in interfaces there is a separate set of factories in 
@scala[`akka.stream.scaladsl.JavaFlowSupport`]@java[`akka.stream.javadsl.JavaFlowSupport`].

In the following samples the standalone Reactive Stream API factories has been used but each such call can be replaced with the
corresponding method from `JavaFlowSupport` and the JDK @scala[`java.util.concurrent.Flow._`]@java[`java.util.concurrent.Flow.*`] interfaces.

Note that it is not possible to use `JavaFlowSupport` on Java 8 since the needed interfaces simply is not available in the Java standard library.

The two most important interfaces in Reactive Streams are the `Publisher` and `Subscriber`.

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #imports }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #imports }

Let us assume that a library provides a publisher of tweets:

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #tweets-publisher }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #tweets-publisher }

and another library knows how to store author handles in a database:

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #author-storage-subscriber }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #author-storage-subscriber }

Using an Akka Streams `Flow` we can transform the stream and connect those:

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #authors #connect-all }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #authors #connect-all }

The `Publisher` is used as an input `Source` to the flow and the
`Subscriber` is used as an output `Sink`.

A `Flow` can also be converted to a `RunnableGraph[Processor[In, Out]]` which
materializes to a `Processor` when `run()` is called. `run()` itself can be called multiple
times, resulting in a new `Processor` instance each time.

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #flow-publisher-subscriber }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #flow-publisher-subscriber }

A publisher can be connected to a subscriber with the `subscribe` method.

It is also possible to expose a `Source` as a `Publisher`
by using the Publisher-`Sink`:

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #source-publisher }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #source-publisher }

A publisher that is created with  @scala[`Sink.asPublisher(fanout = false)`]@java[`Sink.asPublisher(AsPublisher.WITHOUT_FANOUT)`] supports only a single subscription.
Additional subscription attempts will be rejected with an `IllegalStateException`.

A publisher that supports multiple subscribers using fan-out/broadcasting is created as follows:

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #author-alert-subscriber #author-storage-subscriber }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #author-alert-subscriber #author-storage-subscriber }


Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #source-fanoutPublisher }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #source-fanoutPublisher }

The input buffer size of the operator controls how far apart the slowest subscriber can be from the fastest subscriber
before slowing down the stream.

To make the picture complete, it is also possible to expose a `Sink` as a `Subscriber`
by using the Subscriber-`Source`:

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #sink-subscriber }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #sink-subscriber }

It is also possible to use re-wrap `Processor` instances as a `Flow` by
passing a factory function that will create the `Processor` instances:

Scala
:   @@snip [ReactiveStreamsDocSpec.scala](/akka-docs/src/test/scala/docs/stream/ReactiveStreamsDocSpec.scala) { #use-processor }

Java
:   @@snip [ReactiveStreamsDocTest.java](/akka-docs/src/test/java/jdocs/stream/ReactiveStreamsDocTest.java) { #use-processor }

Please note that a factory is necessary to achieve reusability of the resulting `Flow`.


## Other implementations

Implementing Reactive Streams makes it possible to plug Akka Streams together with other stream libraries that adhere to the standard.
An incomplete list of other implementations:

 * [Reactor (1.1+)](https://github.com/reactor/reactor)
 * [RxJava](https://github.com/ReactiveX/RxJavaReactiveStreams)
 * [Ratpack](https://www.ratpack.io/manual/current/streams.html)
 * [Slick](https://scala-slick.org/)
