# Actors interop

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

## Overview

There are various use cases where it might be reasonable to use actors and streams together:

* when integrating existing API's that might be streams- or actors-based.
* when there is any mutable state that should be shared across multiple streams.
* when there is any mutable state or logic that can be influenced 'from outside' while the stream is running.

For piping the elements of a stream as messages to an ordinary actor you can use
`ask` in a `mapAsync` or use `Sink.actorRefWithBackpressure`.

Messages can be sent to a stream with `Source.queue` or via the `ActorRef` that is
materialized by `Source.actorRef`.

Additionally you can use `ActorSource.actorRef`, `ActorSource.actorRefWithBackpressure`, `ActorSink.actorRef` and `ActorSink.actorRefWithBackpressure` shown below.
 
### ask

@@@ note
  See also: @ref[Flow.ask operator reference docs](operators/Source-or-Flow/ask.md), @ref[ActorFlow.ask operator reference docs](operators/ActorFlow/ask.md) for Akka Typed
@@@

A nice way to delegate some processing of elements in a stream to an actor is to use `ask`.
The back-pressure of the stream is maintained by the @scala[`Future`]@java[`CompletionStage`] of
the `ask` and the mailbox of the actor will not be filled with more messages than the given
`parallelism` of the `ask` operator (similarly to how the `mapAsync` operator works).

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #ask }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #ask }

Note that the messages received in the actor will be in the same order as
the stream elements, i.e. the `parallelism` does not change the ordering
of the messages. There is a performance advantage of using parallelism > 1
even though the actor will only process one message at a time because then there
is already a message in the mailbox when the actor has completed previous
message.

The actor must reply to the @scala[`sender()`]@java[`getSender()`] for each message from the stream. That
reply will complete the  @scala[`Future`]@java[`CompletionStage`] of the `ask` and it will be the element that is emitted downstream.

In case the target actor is stopped, the operator will fail with an `AskStageTargetActorTerminatedException`

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #ask-actor }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #ask-actor }

The stream can be completed with failure by sending `akka.actor.Status.Failure` as reply from the actor.

If the `ask` fails due to timeout the stream will be completed with
`TimeoutException` failure. If that is not desired outcome you can use `recover`
on the `ask` @scala[`Future`]@java[`CompletionStage`], or use the other "restart" operators to restart it.

If you don't care about the reply values and only use them as back-pressure signals you
can use `Sink.ignore` after the `ask` operator and then actor is effectively a sink
of the stream.

Note that while you may implement the same concept using `mapAsync`, that style would not be aware of the actor terminating.

If you are intending to ask multiple actors by using @ref:[Actor routers](../routing.md), then
you should use `mapAsyncUnordered` and perform the ask manually in there, as the ordering of the replies is not important,
since multiple actors are being asked concurrently to begin with, and no single actor is the one to be watched by the operator.

### Sink.actorRefWithBackpressure

@@@ note
  See also: @ref[Sink.actorRefWithBackpressure operator reference docs](operators/Sink/actorRefWithBackpressure.md)
@@@

The sink sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
First element is always *onInitMessage*, then stream is waiting for the given acknowledgement message
from the given actor which means that it is ready to process elements. It also requires the given acknowledgement
message after each stream element to make back-pressure work.

If the target actor terminates the stream will be cancelled. When the stream is completed successfully the
given `onCompleteMessage` will be sent to the destination actor. When the stream is completed with
failure a `akka.actor.Status.Failure` message will be sent to the destination actor.

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #actorRefWithBackpressure }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #actorRefWithBackpressure }

The receiving actor would then need to be implemented similar to the following:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #actorRefWithBackpressure-actor }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #actorRefWithBackpressure-actor }

Note that replying to the sender of the elements (the "stream") is required as lack of those ack signals would be interpreted
as back-pressure (as intended), and no new elements will be sent into the actor until it acknowledges some elements.
Handling the other signals while is not required, however is a good practice, to see the state of the stream's lifecycle
in the connected actor as well. Technically it is also possible to use multiple sinks targeting the same actor,
however it is not common practice to do so, and one should rather investigate using a `Merge` operator for this purpose.


@@@ note

Using `Sink.actorRef` or ordinary `tell` from a `map` or `foreach` operator means that there is
no back-pressure signal from the destination actor, i.e. if the actor is not consuming the messages
fast enough the mailbox of the actor will grow, unless you use a bounded mailbox with zero
*mailbox-push-timeout-time* or use a rate limiting operator in front. It's often better to
use `Sink.actorRefWithBackpressure` or `ask` in `mapAsync`, though.

@@@

### Source.queue

`Source.queue` is an improvement over `Sink.actorRef`, since it can provide backpressure.
The `offer` method returns a @scala[`Future`]@java[`CompletionStage`], which completes with the result of the enqueue operation.

`Source.queue` can be used for emitting elements to a stream from an actor (or from anything running outside
the stream). The elements will be buffered until the stream can process them. You can `offer` elements to
the queue and they will be emitted to the stream if there is demand from downstream, otherwise they will
be buffered until request for demand is received.

Use overflow strategy `akka.stream.OverflowStrategy.backpressure` to avoid dropping of elements if the
buffer is full, instead the returned @scala[`Future`]@java[`CompletionStage`] does not complete until there is space in the
buffer and `offer` should not be called again until it completes.

Using `Source.queue` you can push elements to the queue and they will be emitted to the stream if there is
demand from downstream, otherwise they will be buffered until request for demand is received. Elements in the buffer
will be discarded if downstream is terminated.

You could combine it with the @ref[`throttle`](operators/Source-or-Flow/throttle.md) operator is used to slow down the stream to `5 element` per `3 seconds` and other patterns.

`SourceQueue.offer` returns @scala[`Future[QueueOfferResult]`]@java[`CompletionStage<QueueOfferResult>`] which completes with `QueueOfferResult.Enqueued`
if element was added to buffer or sent downstream. It completes with `QueueOfferResult.Dropped` if element
was dropped. Can also complete  with `QueueOfferResult.Failure` - when stream failed or
`QueueOfferResult.QueueClosed` when downstream is completed.

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #source-queue }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #source-queue }

When used from an actor you typically `pipe` the result of the @scala[`Future`]@java[`CompletionStage`] back to the actor to
continue processing.

### Source.actorRef

Messages sent to the actor that is materialized by `Source.actorRef` will be emitted to the
stream if there is demand from downstream, otherwise they will be buffered until request for
demand is received.

Depending on the defined `OverflowStrategy` it might drop elements if there is no space
available in the buffer. The strategy `OverflowStrategy.backpressure` is not supported
for this Source type, i.e. elements will be dropped if the buffer is filled by sending
at a rate that is faster than the stream can consume. You should consider using `Source.queue`
if you want a backpressured actor interface.

The stream can be completed successfully by sending any message to the actor that is handled
by the completion matching function that was provided when the actor reference was created.
If the returned completion strategy is `akka.stream.CompletionStrategy.immediately` the completion will be signaled immediately.
If the completion strategy is `akka.stream.CompletionStrategy.draining`, already buffered elements will be processed before signaling completion.
Any elements that are in the actor's mailbox and subsequent elements sent to the actor will not be processed.

The stream can be completed with failure by sending any message to the
actor that is handled by the failure matching function that was specified
when the actor reference was created.

The actor will be stopped when the stream is completed, failed or cancelled from downstream.
You can watch it to get notified when that happens.


Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #source-actorRef }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #source-actorRef }


### ActorSource.actorRef

Materialize an @java[`ActorRef<T>`]@scala[`ActorRef[T]`]; sending messages to it will emit them on the stream only if they are of the same type as the stream.

@@@ note
  See also: @ref[ActorSource.actorRef operator reference docs](operators/ActorSource/actorRef.md)
@@@

### ActorSource.actorRefWithBackpressure

Materialize an @java[`ActorRef<T>`]@scala[`ActorRef[T]`]; sending messages to it will emit them on the stream. The source acknowledges reception after emitting a message, to provide back pressure from the source.

@@@ note
  See also: @ref[ActorSource.actorRefWithBackpressure operator reference docs](operators/ActorSource/actorRefWithBackpressure.md)
@@@

### ActorSink.actorRef

Sends the elements of the stream to the given @java[`ActorRef<T>`]@scala[`ActorRef[T]`], without considering backpressure.

@@@ note
  See also: @ref[ActorSink.actorRef operator reference docs](operators/ActorSink/actorRef.md)
@@@

### ActorSink.actorRefWithBackpressure

Sends the elements of the stream to the given @java[`ActorRef<T>`]@scala[`ActorRef[T]`] with backpressure, to be able to signal demand when the actor is ready to receive more elements.

@@@ note
  See also: @ref[ActorSink.actorRefWithBackpressure operator reference docs](operators/ActorSink/actorRefWithBackpressure.md)
@@@


### Topic.source

A source that will subscribe to a @apidoc[akka.actor.typed.pubsub.Topic$] and stream messages published to the topic.

@@@ note
See also: @ref[ActorSink.actorRefWithBackpressure operator reference docs](operators/PubSub/source.md)
@@@

### Topic.sink

A sink that will publish emitted messages to a @apidoc[akka.actor.typed.pubsub.Topic$].

@@@ note
See also: @ref[ActorSink.actorRefWithBackpressure operator reference docs](operators/PubSub/sink.md)
@@@
