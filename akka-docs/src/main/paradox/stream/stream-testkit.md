# Testing streams

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Stream TestKit, add the module to your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-stream-testkit_$scala.binary.version$"
  version=AkkaVersion
  scope="test"
}

## Introduction

Verifying behavior of Akka Stream sources, flows and sinks can be done using
various code patterns and libraries. Here we will discuss testing these
elements using:

 * simple sources, sinks and flows;
 * sources and sinks in combination with @apidoc[akka.testkit.TestProbe] from the `akka-testkit` module;
 * sources and sinks specifically crafted for writing tests from the `akka-stream-testkit` module.

It is important to keep your data processing pipeline as separate sources,
flows and sinks. This makes them testable by wiring them up to other
sources or sinks, or some test harnesses that `akka-testkit` or
`akka-stream-testkit` provide.

## Built-in sources, sinks and operators

Testing a custom sink can be as simple as attaching a source that emits
elements from a predefined collection, running a constructed test flow and
asserting on the results that sink produced. Here is an example of a test for a
sink:

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #strict-collection }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #strict-collection }

The same strategy can be applied for sources as well. In the next example we
have a source that produces an infinite stream of elements. Such source can be
tested by asserting that first arbitrary number of elements hold some
condition. Here the @apidoc[take](akka.stream.*.Source) {scala="#take(n:Long):FlowOps.this.Repr[Out]" java="#take(long)"} operator and @apidoc[Sink.seq](akka.stream.*.Sink$) {scala="#seq[T]:akka.stream.scaladsl.Sink[T,scala.concurrent.Future[Seq[T]]]" java="#seq()"} are very useful.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #grouped-infinite }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #grouped-infinite }

When testing a flow we need to attach a source and a sink. As both stream ends
are under our control, we can choose sources that tests various edge cases of
the flow and sinks that ease assertions.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #folded-stream }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #folded-stream }

## TestKit

Akka Stream offers integration with Actors out of the box. This support can be
used for writing stream tests that use familiar @apidoc[akka.testkit.TestProbe] from the
`akka-testkit` API.

One of the more straightforward tests would be to materialize stream to a
@scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] and then use @scala[@scaladoc[pipe](akka.pattern.PipeToSupport#pipe[T](future:scala.concurrent.Future[T])(implicitexecutionContext:scala.concurrent.ExecutionContext):PipeToSupport.this.PipeableFuture[T])]@java[@scaladoc[Patterns.pipe](akka.pattern.Patterns$#pipe[T](future:java.util.concurrent.CompletionStage[T],context:scala.concurrent.ExecutionContext):akka.pattern.PipeableCompletionStage[T])] pattern to pipe the result of that future
to the probe.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #pipeto-testprobe }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #pipeto-testprobe }

Instead of materializing to a future, we can use a @apidoc[Sink.actorRef](akka.stream.*.Sink$) {scala="#actorRef[T](ref:akka.actor.ActorRef,onCompleteMessage:Any,onFailureMessage:Throwable=%3EAny):akka.stream.scaladsl.Sink[T,akka.NotUsed]" java="#actorRef(akka.actor.ActorRef,java.lang.Object)"} that
sends all incoming elements to the given @apidoc[akka.actor.ActorRef]. Now we can use
assertion methods on @apidoc[akka.testkit.TestProbe] and expect elements one by one as they
arrive. We can also assert stream completion by expecting for
`onCompleteMessage` which was given to `Sink.actorRef`.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #sink-actorref }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #sink-actorref }

Similarly to `Sink.actorRef` that provides control over received
elements, we can use @apidoc[Source.actorRef](akka.stream.*.Source$) {scala="#actorRef[T](completionMatcher:PartialFunction[Any,akka.stream.CompletionStrategy],failureMatcher:PartialFunction[Any,Throwable],bufferSize:Int,overflowStrategy:akka.stream.OverflowStrategy):akka.stream.scaladsl.Source[T,akka.actor.ActorRef]" java="#actorRef(int,akka.stream.OverflowStrategy)"} and have full control over
elements to be sent.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #source-actorref }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #source-actorref }

## Streams TestKit

You may have noticed various code patterns that emerge when testing stream
pipelines. Akka Stream has a separate `akka-stream-testkit` module that
provides tools specifically for writing stream tests. This module comes with
two main components that are @apidoc[akka.stream.testkit.*.TestSource$] and @apidoc[akka.stream.testkit.*.TestSink$] which
provide sources and sinks that materialize to probes that allow fluent API.

### Using the TestKit

A sink returned by @apidoc[TestSink.probe](akka.stream.testkit.*.TestSink$) {scala="#probe[T](implicitsystem:akka.actor.ActorSystem):akka.stream.scaladsl.Sink[T,akka.stream.testkit.TestSubscriber.Probe[T]]" java="#probe(akka.actor.ActorSystem)"} allows manual control over demand and
assertions over elements coming downstream.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #test-sink-probe }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #test-sink-probe }

A source returned by @apidoc[TestSource.probe](akka.stream.testkit.*.TestSource$) {scala="#probe[T](implicitsystem:akka.actor.ActorSystem):akka.stream.scaladsl.Source[T,akka.stream.testkit.TestPublisher.Probe[T]]" java="#probe(akka.actor.ActorSystem)"} can be used for asserting demand or
controlling when stream is completed or ended with an error.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #test-source-probe }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #test-source-probe }

You can also inject exceptions and test sink behavior on error conditions.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #injecting-failure }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #injecting-failure }

Test source and sink can be used together in combination when testing flows.

Scala
:   @@snip [StreamTestKitDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamTestKitDocSpec.scala) { #test-source-and-sink }

Java
:   @@snip [StreamTestKitDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamTestKitDocTest.java) { #test-source-and-sink }

## Fuzzing Mode

For testing, it is possible to enable a special stream execution mode that exercises concurrent execution paths
more aggressively (at the cost of reduced performance) and therefore helps exposing race conditions in tests. To
enable this setting add the following line to your configuration:

```
akka.stream.materializer.debug.fuzzing-mode = on
```

@@@ warning

Never use this setting in production or benchmarks. This is a testing tool to provide more coverage of your code
during tests, but it reduces the throughput of streams. A warning message will be logged if you have this setting
enabled.

@@@
