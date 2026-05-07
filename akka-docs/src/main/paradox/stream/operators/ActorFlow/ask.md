# ActorFlow.ask

Use the "Ask Pattern" to send each stream element as an `ask` to the target actor (of the new actors API), and expect a reply that will be emitted downstream.

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

@apidoc[ActorFlow.ask](ActorFlow$) { scala="#ask%5BI,Q,A](ref:akka.actor.typed.ActorRef%5BQ])(makeMessage:(I,akka.actor.typed.ActorRef%5BA])=%3EQ)(implicittimeout:akka.util.Timeout):akka.stream.scaladsl.Flow%5BI,A,akka.NotUsed]" java="#ask(akka.actor.typed.ActorRef,java.time.Duration,java.util.function.BiFunction)" }

## Description

Use the @ref[Ask pattern](../../../typed/interaction-patterns.md#request-response-with-ask-from-outside-an-actor) to send a request-reply message to the target `ref` actor.
If any of the asks times out it will fail the stream with an @apidoc[AskTimeoutException].

The `ask` operator requires

* the actor `ref`,
* a `makeMessage` function to create the message sent to the actor from the incoming element, and the actor ref accepting the actor's reply message 
* a timeout.

See also:

* @ref[Flow.ask](../Source-or-Flow/ask.md) for the classic actors variant

## Examples

The `ActorFlow.ask` sends a message to the actor. The actor expects `Asking` messages which contain the actor ref for replies of type `Reply`. When the actor for replies receives a reply, the `ActorFlow.ask` stream stage emits the reply and the `map` extracts the message `String`.

Scala
:  @@snip [ask.scala](/akka-stream-typed/src/test/scala/docs/scaladsl/ActorFlowSpec.scala) { #imports #ask-actor #ask }

Java
:   @@snip [ask.java](/akka-stream-typed/src/test/java/docs/javadsl/ActorFlowCompileTest.java) { #ask-actor #ask }


## Reactive Streams semantics

@@@div { .callout }

**emits** when the futures (in submission order) created by the ask pattern internally are completed

**backpressures** when the number of futures reaches the configured parallelism and the downstream backpressures

**completes** when upstream completes and all futures have been completed and all elements have been emitted

**fails** when the passed-in actor terminates, or when any of the `ask`s exceed a timeout

**cancels** when downstream cancels

@@@
