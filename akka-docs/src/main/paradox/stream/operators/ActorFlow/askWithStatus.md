# ActorFlow.askWithStatus

Use the "Ask Pattern" to send each stream element as an `ask` to the target actor (of the new actors API),  and expect a reply of Type @scala[`StatusReply[T]`]@java[`StatusReply<T>`] where the T will be unwrapped and emitted downstream.

@ref[Actor interop operators](../index.md#actor-interop-operators)

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

This operator is included in:

@@dependency[sbt,Maven,Gradle] {
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-stream-typed_$scala.binary.version$"
  version=AkkaVersion
}

## Signature

@apidoc[ActorFlow.askWithStatus](ActorFlow$) { scala="#askWithStatus[I,Q,A](parallelism:Int)(ref:akka.actor.typed.ActorRef[Q])(makeMessage:(I,akka.actor.typed.ActorRef[akka.pattern.StatusReply[A]])=&gt;Q)(implicittimeout:akka.util.Timeout):akka.stream.scaladsl.Flow[I,A,akka.NotUsed]" java="#askWithStatus[I,Q,A](parallelism:Int,ref:akka.actor.typed.ActorRef[Q],timeout:java.time.Duration,makeMessage:java.util.function.BiFunction[I,akka.actor.typed.ActorRef[akka.pattern.StatusReply[A]],Q]):akka.stream.javadsl.Flow[I,A,akka.NotUsed]" }
@apidoc[ActorFlow.askWithStatus](ActorFlow$) { scala="#askWithStatus[I,Q,A](ref:akka.actor.typed.ActorRef[Q])(makeMessage:(I,akka.actor.typed.ActorRef[akka.pattern.StatusReply[A]])=&gt;Q)(implicittimeout:akka.util.Timeout):akka.stream.scaladsl.Flow[I,A,akka.NotUsed]" java="#askWithStatus[I,Q,A](ref:akka.actor.typed.ActorRef[Q],timeout:java.time.Duration,makeMessage:java.util.function.BiFunction[I,akka.actor.typed.ActorRef[akka.pattern.StatusReply[A]],Q]):akka.stream.javadsl.Flow[I,A,akka.NotUsed]" }

## Description

Use the @ref[Ask pattern](../../../typed/interaction-patterns.md#request-response-with-ask-from-outside-an-actor) to send a request-reply message to the target `ref` actor when you expect the reply to be `akka.pattern.StatusReply`.
If any of the asks times out it will fail the stream with an @apidoc[AskTimeoutException].

The `askWithStatus` operator requires

* the actor `ref`,
* a `makeMessage` function to create the message sent to the actor from the incoming element, and the actor ref accepting the actor's reply message 
* a timeout.


## Examples

The `ActorFlow.askWithStatus` sends a message to the actor. The actor expects `AskingWithStatus` messages which contain the actor ref for replies of type @scala[`StatusReply[String]`]@java[`StatusReply<String>`]. When the actor for replies receives a reply, the `ActorFlow.askWithStatus` stream stage emits the reply and the `map` extracts the message `String`.

Scala
:  @@snip [ask.scala](/akka-stream-typed/src/test/scala/docs/scaladsl/ActorFlowSpec.scala) { #imports #ask-actor #ask }

Java
:   @@snip [ask.java](/akka-stream-typed/src/test/java/docs/javadsl/ActorFlowCompileTest.java) { #ask-actor #ask }


## Reactive Streams semantics

@@@div { .callout }

**emits** when the futures (in submission order) created by the ask pattern internally are completed

**backpressures** when the number of futures reaches the configured parallelism and the downstream backpressures

**completes** when upstream completes and all futures have been completed and all elements have been emitted

**fails** when the passed-in actor terminates, or when any of the `askWithStatus`s exceed a timeout

**cancels** when downstream cancels

@@@
