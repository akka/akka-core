# ActorSink.actorRefWithBackpressure

Sends the elements of the stream to the given @java[`ActorRef<T>`]@scala[`ActorRef[T]`] of the new actors API with backpressure, to be able to signal demand when the actor is ready to receive more elements.

@ref[Actor interop operators](../index.md#actor-interop-operators)

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

@apidoc[ActorSink.actorRefWithBackpressure](ActorSink$) { scala="#actorRefWithBackpressure[T,M,A](ref:akka.actor.typed.ActorRef[M],messageAdapter:(akka.actor.typed.ActorRef[A],T)=&gt;M,onInitMessage:akka.actor.typed.ActorRef[A]=&gt;M,ackMessage:A,onCompleteMessage:M,onFailureMessage:Throwable=&gt;M):akka.stream.scaladsl.Sink[T,akka.NotUsed]" java="#actorRefWithBackpressure(akka.actor.typed.ActorRef,akka.japi.function.Function2,akka.japi.function.Function,java.lang.Object,java.lang.Object,akka.japi.function.Function)" }

## Description

Sends the elements of the stream to the given @java[`ActorRef<T>`]@scala[`ActorRef[T]`] with backpressure, to be able to signal demand when the actor is ready to receive more elements.
There is also a variant without a concrete acknowledge message accepting any message as such.

See also:

* @ref[`ActorSink.actorRef`](../ActorSink/actorRefWithBackpressure.md) Send elements to an actor of the new actors API, without considering backpressure
* @ref[`Sink.actorRef`](../Sink/actorRef.md) Send elements to an actor of the classic actors API, without considering backpressure
* @ref[`Sink.actorRefWithBackpressue`](../Sink/actorRefWithBackpressure.md) The corresponding operator for the classic actors API

## Examples

Scala
:  @@snip [ActorSourceSinkExample.scala](/akka-stream-typed/src/test/scala/docs/akka/stream/typed/ActorSourceSinkExample.scala) { #actor-sink-ref-with-backpressure }

Java
:  @@snip [ActorSinkWithAckExample.java](/akka-stream-typed/src/test/java/docs/akka/stream/typed/ActorSinkWithAckExample.java) { #actor-sink-ref-with-backpressure }

## Reactive Streams semantics

@@@div { .callout }

**cancels** when the actor terminates

**backpressures** when the actor acknowledgement has not arrived

@@@
