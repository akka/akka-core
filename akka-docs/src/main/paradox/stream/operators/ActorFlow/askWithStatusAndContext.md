# ActorFlow.askWithContext

Use the "Ask Pattern" to send each stream element (without the context) as an `ask` to the target actor (of the new actors API), and expect a reply of Type @scala[`StatusReply[T]`]@java[`StatusReply<T>`] where the T will be unwrapped and emitted downstream.

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

@apidoc[ActorFlow.askWithStatusAndContext](ActorFlow$) { scala="#askWithStatusAndContext[I,Q,A,Ctx](parallelism:Int)(ref:akka.actor.typed.ActorRef[Q])(makeMessage:(I,akka.actor.typed.ActorRef[akka.pattern.StatusReply[A]])=&gt;Q)(implicittimeout:akka.util.Timeout):akka.stream.scaladsl.Flow[(I,Ctx),(A,Ctx),akka.NotUsed]" java="#askWithStatusAndContext[I,Q,A,Ctx](parallelism:Int,ref:akka.actor.typed.ActorRef[Q],timeout:java.time.Duration,makeMessage:java.util.function.BiFunction[I,akka.actor.typed.ActorRef[akka.pattern.StatusReply[A]],Q])" }

## Description

Use the @ref[Ask pattern](../../../typed/interaction-patterns.md#request-response-with-ask-from-outside-an-actor) to send a request-reply message to the target `ref` actor when you expect the reply to be `akka.pattern.StatusReply`.
The stream context is not sent, instead it is locally recombined to the actor's reply.

If any of the asks times out it will fail the stream with an @apidoc[AskTimeoutException].

The `ask` operator requires

* the actor `ref`,
* a `makeMessage` function to create the message sent to the actor from the incoming element, and the actor ref accepting the actor's reply message 
* a timeout.

## Reactive Streams semantics

@@@div { .callout }

**emits** when the futures (in submission order) created by the ask pattern internally are completed

**backpressures** when the number of futures reaches the configured parallelism and the downstream backpressures

**completes** when upstream completes and all futures have been completed and all elements have been emitted

**fails** when the passed-in actor terminates, or when any of the `ask`s exceed a timeout

**cancels** when downstream cancels

@@@
