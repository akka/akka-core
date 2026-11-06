---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.9 or earlier, or in Akka 2.10.0, have been removed. The sections below list the removed APIs and their replacements.

### akka-stream

* `ActorMaterializer`: `Materializer` has all the methods `ActorMaterializer` had. Use the system materializer, which is available implicitly from an `ActorSystem` or `ActorContext`, or `Materializer(system)` to create a separate one.
* The `ActorMaterializerSettings`, `IOSettings` and `StreamRefSettings` factories and `with*` methods, and `Materializer.settings`: configure streams with the `akka.stream.materializer` config, or per stream with `ActorAttributes`, `TcpAttributes` and `StreamRefAttributes`. Operators read the settings from their attributes.
* `IOResult.status`, `wasSuccessful`, `getError` and `createFailed`, and `AbruptIOTerminationException`: IO operators fail the materialized value with `IOOperationIncompleteException`. `FileIO.fromPath` now also fails the materialized value when the file can't be read, instead of completing it with a failed status.
* The `copy`, `unapply` and `Product` methods of `Attributes`: use the explicit methods on `Attributes`. `Attributes` no longer extends `Product`.
* `Source.fromFuture`, `fromCompletionStage`, `fromFutureSource` and `fromSourceCompletionStage`: use `Source.future`, `Source.completionStage`, `Source.futureSource` and `Source.completionStageSource`.
* `Source.lazily` and `Source.lazilyAsync`: use `Source.lazySource`, `Source.lazyFuture` or `Source.lazyCompletionStage`.
* `Sink.lazyInit` and `Sink.lazyInitAsync`: use `Sink.lazyFutureSink` or `Sink.lazyCompletionStageSink`. `Flow.lazyInit` and `Flow.lazyInitAsync`: use `Flow.lazyFutureFlow` or `Flow.lazyCompletionStageFlow`.
* `Source.setup`, `Flow.setup` and `Sink.setup`: use `fromMaterializer`.
* `Source.actorRef(bufferSize, overflowStrategy)` and `Sink.actorRef(ref, onCompleteMessage)`: use the overloads that take completion and failure matchers, or an on-complete and an on-failure message. `Source.actorRefWithAck` and `Sink.actorRefWithAck`: use `actorRefWithBackpressure`.
* `FileIO.fromFile` and `FileIO.toFile`: use `FileIO.fromPath` and `FileIO.toPath`.
* The `akka.stream.javadsl.Tcp.bind` and `outgoingConnection` overloads that take a `scala.concurrent.duration.Duration`: use the overloads that take a `java.time.Duration`.
* The `RestartSource`, `RestartFlow` and `RestartSink` overloads without `RestartSettings`: use the overloads that take `RestartSettings`.
* `TimerGraphStageLogic.schedulePeriodically`, `schedulePeriodicallyWithInitialDelay` and `Materializer.schedulePeriodically`: use `scheduleWithFixedDelay` or `scheduleAtFixedRate`.
* The `BindFailedException` object: match on the `BindFailedException` class instead.
