# PubSub.source

A source that will subscribe to a @apidoc[akka.actor.typed.pubsub.Topic$] and stream messages published to the topic. 

@ref[Actor interop operators](../index.md#actor-interop-operators)

The source can be materialized  multiple times, each materialized stream will stream messages published to the topic after the stream has started.

Note that it is not possible to propagate the backpressure from the running stream to the pub sub topic,
if the stream is backpressuring published messages are buffered up to a limit and if the limit is hit
the configurable `OverflowStrategy` decides what happens. It is not possible to use the `Backpressure`
strategy.


## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@


This operator is included in:

@@dependency[sbt,Maven,Gradle] {
bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
symbol1=AkkaVersion
value1="$akka.version$"
group="com.typesafe.akka"
artifact="akka-stream-typed_$scala.binary.version$"
version=AkkaVersion
}

## Signature

@apidoc[PubSub.source](akka.stream.typed.*.PubSub$) { scala="#source[T](topic:akka.actor.typed.Toppic[T]):akka.stream.scaladsl.Source[T,akka.NotUsed]" java="#source(akka.actor.typed.Topic)" }

## Reactive Streams semantics

@@@div { .callout }

**emits** a message published to the topic is emitted as soon as there is demand from downstream

**completes** when the topic actor terminates 

@@@
