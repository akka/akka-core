# PubSub.sink

A sink that will publish emitted messages to a @apidoc[akka.actor.typed.pubsub.Topic$].

@ref[Actor interop operators](../index.md#actor-interop-operators)

Note that there is no backpressure from the topic, so care must be taken to not publish messages at a higher rate than that can be handled 
by subscribers.

If the topic does not have any subscribers when a message is published, or the topic actor is stopped, the message is sent to dead letters.

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

@apidoc[PubSub.sink](akka.stream.typed.*.PubSub$) { scala="#sink[T](topic:akka.actor.typed.Toppic[T]):akka.stream.scaladsl.Sink[T,akka.NotUsed]" java="#sink(akka.actor.typed.Topic)" }

## Reactive Streams semantics

@@@div { .callout }

**cancels** never

**backpressures** never

@@@
