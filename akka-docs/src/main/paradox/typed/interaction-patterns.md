# Interaction Patterns

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Actors](../actors.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Actor Typed, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-actor-typed_$scala.binary.version$
  version=AkkaVersion
}

## Introduction

Interacting with an Actor in Akka is done through an @scala[@scaladoc[ActorRef[T]](akka.actor.typed.ActorRef)]@java[@javadoc[ActorRef<T>](akka.actor.typed.ActorRef)] where `T` is the type of messages the actor accepts, also known as the "protocol". This ensures that only the right kind of messages can be sent to an actor and also that no one else but the Actor itself can access the Actor instance internals.

Message exchange with Actors follow a few common patterns, let's go through each one of them. 

## Fire and Forget

The fundamental way to interact with an actor is through @scala["tell", which is so common that it has a special symbolic method name: `actorRef` @scaladoc[!](akka.actor.typed.ActorRef#tell(msg:T):Unit) `message`]@java[@javadoc[actorRef.tell(message)](akka.actor.typed.ActorRef#tell(T))]. Sending a message with tell can safely be done from any thread.

Tell is asynchronous which means that the method returns right away. After the statement is executed there is no guarantee that the message has been processed by the recipient yet. It also means there is no way to know if the message was received, the processing succeeded or failed.

**Example:**

![fire-forget.png](./images/fire-forget.png)

With the given protocol and actor behavior:

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #fire-and-forget-definition }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #fire-and-forget-definition }


Fire and forget looks like this:

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #fire-and-forget-doit }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #fire-and-forget-doit }


**Useful when:**

 * It is not critical to be sure that the message was processed
 * There is no way to act on non successful delivery or processing
 * We want to minimize the number of messages created to get higher throughput (sending a response would require creating twice the number of messages)

**Problems:**

 * If the inflow of messages is higher than the actor can process the inbox will fill up and can in the worst case cause the JVM crash with an @javadoc[OutOfMemoryError](java.lang.OutOfMemoryError)
 * If the message gets lost, the sender will not know

## Request-Response

Many interactions between actors require one or more response message being sent back from the receiving actor. A response message can be a result of a query, some form of acknowledgment that the message was received and processed or events that the request subscribed to.

In Akka the recipient of responses has to be encoded as a field in the message itself, which the recipient can then use to send (tell) a response back.

**Example:**

![request-response.png](./images/request-response.png)

With the following protocol:

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #request-response-protocol }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #request-response-protocol }


The sender would use its own @scala[`ActorRef[Response]`]@java[`ActorRef<Response>`], which it can access through @scala[@scaladoc[ActorContext.self](akka.actor.typed.scaladsl.ActorContext#self:akka.actor.typed.ActorRef[T])]@java[@javadoc[ActorContext.getSelf()](akka.actor.typed.javadsl.ActorContext#getSelf())], for the `replyTo`. 

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #request-response-send }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #request-response-send }


On the receiving side the @scala[`ActorRef[Response]`]@java[`ActorRef<Response>`] can then be used to send one or more responses back:

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #request-response-respond }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #request-response-respond }


**Useful when:**

 * Subscribing to an actor that will send many response messages back
 
**Problems:**

 * Actors seldom have a response message from another actor as a part of their protocol (see @ref:[adapted response](#adapted-response))
 * It is hard to detect that a message request was not delivered or processed (see @ref:[ask](#request-response-with-ask-between-two-actors))
 * Unless the protocol already includes a way to provide context, for example a request id that is also sent in the
   response, it is not possible to tie an interaction to some specific context without introducing a new,
   separate, actor (see @ref:[ask](#request-response-with-ask-between-two-actors) or @ref:[per session child actor](#per-session-child-actor))

@@@ div { .group-scala }

### Request response with Scala 3

Scala 3 introduces union types, allowing for ad hoc combinations of types, this can be leveraged for response message
types instead of the message adapters. The behavior is internally declared as union of its own protocol and any response
messages it may accept.

The public protocol that the actor accepts by returning `Behavior[Command]` stays the same by use of `.narrow`:

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala-3/docs/akka/typed/InteractionPatternsScala3Spec.scala) { #request-response-send }

@@@

## Adapted Response

Most often the sending actor does not, and should not, support receiving the response messages of another actor. In such cases we need to provide an @apidoc[akka.actor.typed.ActorRef] of the right type and adapt the response message to a type that the sending actor can handle.

**Example:**

![adapted-response.png](./images/adapted-response.png)

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #adapted-response }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #adapted-response }

You can register several message adapters for different message classes.
It's only possible to have one message adapter per message class to make sure
that the number of adapters are not growing unbounded if registered repeatedly.
That also means that a registered adapter will replace an existing adapter for
the same message class.

A message adapter will be used if the message class matches the given class or
is a subclass thereof. The registered adapters are tried in reverse order of
their registration order, i.e. the last registered first.

A message adapter (and the returned @apidoc[akka.actor.typed.ActorRef]) has the same lifecycle as
the receiving actor. It's recommended to register the adapters in a top level
@apidoc[Behaviors.setup](typed.*.Behaviors$) {scala="#setup[T](factory:akka.actor.typed.scaladsl.ActorContext[T]=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#setup(akka.japi.function.Function)"} or constructor of @apidoc[akka.actor.typed.*.AbstractBehavior] but it's possible to
register them later if needed.

The adapter function is running in the receiving actor and can safely access its state, but if it throws an exception the actor is stopped.

**Useful when:**

 * Translating between different actor message protocols
 * Subscribing to an actor that will send many response messages back
 
**Problems:**

 * It is hard to detect that a message request was not delivered or processed (see @ref:[ask](#request-response-with-ask-between-two-actors))
 * Only one adaption can be made per response message type, if a new one is registered the old one is replaced,
   for example different target actors can't have different adaption if they use the same response types, unless some
   correlation is encoded in the messages
 * Unless the protocol already includes a way to provide context, for example a request id that is also sent in the
   response, it is not possible to tie an interaction to some specific context without introducing a new,
   separate, actor

@@@ div { .group-scala }

### Responses with Scala 3

Scala 3 introduces union types, allowing for ad hoc combinations of types, this can be leveraged for response message
types instead of the message adapters. The behavior is internally declared as union of its own protocol and any response
messages it may accept.

The public protocol that the actor accepts by returning `Behavior[Command]` stays the same by use of `.narrow`:

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala-3/docs/akka/typed/InteractionPatternsScala3Spec.scala) { #adapted-response }

@@@ 


## Request-Response with ask between two actors
 
In an interaction where there is a 1:1 mapping between a request and a response we can use `ask` on the @apidoc[akka.actor.typed.*.ActorContext] to interact with another actor.

The interaction has two steps, first we need to construct the outgoing message, to do that we need an @scala[`ActorRef[Response]`]@java[`ActorRef<Response>`] to put as recipient in the outgoing message. 
The second step is to transform the successful `Response` or failure into a message that is part of the protocol of the sending actor.
See also the [Generic response wrapper](#generic-response-wrapper) for replies that are either a success or an error.

**Example:**

![ask-from-actor.png](./images/ask-from-actor.png)

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #actor-ask }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #actor-ask }


The response adapting function is running in the receiving actor and can safely access its state, but if it throws an exception the actor is stopped.

**Useful when:**

 * Single response queries
 * An actor needs to know that the message was processed before continuing 
 * To allow an actor to resend if a timely response is not produced
 * To keep track of outstanding requests and not overwhelm a recipient with messages ("backpressure")
 * Context should be attached to the interaction but the protocol does not support that (request id, what query the response was for)
 
**Problems:**

 * There can only be a single response to one `ask` (see @ref:[per session child Actor](#per-session-child-actor))
 * When `ask` times out, the receiving actor does not know and may still process it to completion, or even start processing it after the fact
 * Finding a good value for the timeout, especially when `ask` triggers chained `ask`s in the receiving actor. You want a short timeout to be responsive and answer back to the requester, but at the same time you do not want to have many false positives

<a id="outside-ask"></a>
## Request-Response with ask from outside an Actor

Sometimes you need to interact with actors from the outside of the actor system, this can be done with fire-and-forget as described above or through another version of `ask` that returns a @scala[@scaladoc[Future[Response]](scala.concurrent.Future)]@java[@javadoc[CompletionStage<Response>](java.util.concurrent.CompletionStage)] that is either completed with a successful response or failed with a @javadoc[TimeoutException](java.util.concurrent.TimeoutException) if there was no response within the specified timeout.
 
@scala[To do this we use `ask` (or the symbolic `?`) implicitly added to @scaladoc[ActorRef](akka.actor.typed.ActorRef) by `akka.actor.typed.scaladsl.AskPattern._`
to send a message to an actor and get a `Future[Response]` back. `ask` takes implicit @scaladoc[Timeout](akka.util.Timeout) and @scaladoc[ActorSystem](akka.actor.typed.ActorSystem) parameters.]
@java[To do this we use `akka.actor.typed.javadsl.AskPattern.ask` to send a message to an actor and get a 
`CompletionState[Response]` back.]

**Example:**

![ask-from-outside.png](./images/ask-from-outside.png)

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #standalone-ask }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #standalone-ask }

Note that validation errors are also explicit in the message protocol. The `GiveMeCookies` request can reply
with `Cookies` or `InvalidRequest`. The requestor has to decide how to handle an `InvalidRequest` reply. Sometimes
it should be treated as a failed @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] and for that the reply can be mapped on the
requestor side. See also the [Generic response wrapper](#generic-response-wrapper) for replies that are either a success or an error.

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #standalone-ask-fail-future }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #standalone-ask-fail-future }

**Useful when:**

 * Querying an actor from outside of the actor system 

**Problems:**

 * It is easy to accidentally close over and unsafely mutable state with the callbacks on the returned @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] as those will be executed on a different thread
 * There can only be a single response to one `ask` (see @ref:[per session child Actor](#per-session-child-actor))
 * When `ask` times out, the receiving actor does not know and may still process it to completion, or even start processing it after the fact

## Generic response wrapper

In many cases the response can either be a successful result or an error (a validation error that the command was invalid for example).
Having to define two response classes and a shared supertype for every request type can be repetitive, especially in a cluster context 
where you also have to make sure the messages can be serialized to be sent over the network.

To help with this a generic status-response type is included in Akka: @apidoc[StatusReply], everywhere where `ask` can be used
there is also a second method @apidoc[askWithStatus](typed.*.ActorFlow$) {scala="#askWithStatus[I,Q,A](parallelism:Int)(ref:akka.actor.typed.ActorRef[Q])(makeMessage:(I,akka.actor.typed.ActorRef[akka.pattern.StatusReply[A]])=%3EQ)(implicittimeout:akka.util.Timeout):akka.stream.scaladsl.Flow[I,A,akka.NotUsed]" java="#askWithStatus(int,akka.actor.typed.ActorRef,java.time.Duration,java.util.function.BiFunction)"} which, given that the response is a `StatusReply` will unwrap successful responses
and help with handling validation errors. Akka includes pre-built serializers for the type, so in the normal use case a clustered 
application only needs to provide a serializer for the successful result.

For the case where the successful reply does not contain an actual value but is more of an acknowledgment there is a pre defined
@scala[@scaladoc[StatusReply.Ack](akka.pattern.StatusReply$#Ack:akka.pattern.StatusReply[akka.Done])]@java[@javadoc[StatusReply.ack()](akka.pattern.StatusReply$#ack())] of type @scala[`StatusReply[Done]`]@java[`StatusReply<Done>`].

Errors are preferably sent as a text describing what is wrong, but using exceptions to attach a type is also possible.

**Example actor to actor ask:**

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #actor-ask-with-status }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsAskWithStatusTest.java) { #actor-ask-with-status }

A validation error is turned into a `Failure` for the message adapter. In this case we are explicitly handling the validation error separately from
other ask failures.

**Example ask from the outside:**

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #standalone-ask-with-status }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsAskWithStatusTest.java) { #standalone-ask-with-status }

Note that validation errors are also explicit in the message protocol, but encoded as the wrapper type, constructed using @scala[@scaladoc[StatusReply.Error(text)](akka.pattern.StatusReply$$Error$#apply[T](errorMessage:String):akka.pattern.StatusReply[T])]@java[@javadoc[StatusReply.error(text)](akka.pattern.StatusReply$#error(java.lang.String))]:

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #standalone-ask-with-status-fail-future }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsAskWithStatusTest.java) { #standalone-ask-with-status-fail-future }


## Ignoring replies

In some situations an actor has a response for a particular request message but you are not interested in the response. In this case you can pass @scala[@scaladoc[system.ignoreRef](akka.actor.typed.ActorSystem#ignoreRef[U]:akka.actor.typed.ActorRef[U])]@java[@javadoc[system.ignoreRef()](akka.actor.typed.ActorSystem#ignoreRef())] turning the request-response into a fire-and-forget.

@scala[`system.ignoreRef`]@java[`system.ignoreRef()`], as the name indicates, returns an @apidoc[akka.actor.typed.ActorRef] that ignores any message sent to it.

With the same protocol as the @ref[request response](#request-response) above, if the sender would prefer to ignore the reply it could pass @scala[`system.ignoreRef`]@java[`system.ignoreRef()`] for the `replyTo`, which it can access through @scala[`ActorContext.system.ignoreRef`]@java[`ActorContext.getSystem().ignoreRef()`]. 

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #ignore-reply }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #ignore-reply }

**Useful when:**

  * Sending a message for which the protocol defines a reply, but you are not interested in getting the reply

**Problems:**

The returned @apidoc[akka.actor.typed.ActorRef] ignores all messages sent to it, therefore it should be used carefully.
 
 * Passing it around inadvertently as if it was a normal `ActorRef` may result in broken actor-to-actor interactions.
 * Using it when performing an `ask` from outside the Actor System will cause the @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] returned by the `ask` to timeout since it will never complete.
 * Finally, it's legal to @apidoc[watch](typed.*.ActorContext) {scala="#watch[U](other:akka.actor.typed.ActorRef[U]):Unit" java="#watch(akka.actor.typed.ActorRef)"} it, but since it's of a special kind, it never terminates and therefore you will never receive a @apidoc[akka.actor.typed.Terminated] signal from it.

## Send Future result to self

When using an API that returns a @scala[`Future`]@java[`CompletionStage`] from an actor it's common that you would
like to use the value of the response in the actor when the @scala[`Future`]@java[`CompletionStage`] is completed. For
this purpose the `ActorContext` provides a @apidoc[pipeToSelf](typed.*.ActorContext) {scala="#pipeToSelf[Value](future:scala.concurrent.Future[Value])(mapResult:scala.util.Try[Value]=%3ET):Unit" java="#pipeToSelf(java.util.concurrent.CompletionStage,akka.japi.function.Function2)"} method.

**Example:**

![pipe-to-self.png](./images/pipe-to-self.png)

An actor, `CustomerRepository`, is invoking a method on `CustomerDataAccess` that returns a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)].

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #pipeToSelf }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #pipeToSelf }

It could be tempting to just use @scala[`onComplete on the Future`]@java[`a callback on the CompletionStage`], but
that introduces the risk of accessing internal state of the actor that is not thread-safe from an external thread.
For example, the `numberOfPendingOperations` counter in above example can't be accessed from such callback.
Therefore it is better to map the result to a message and perform further processing when receiving that message.

**Useful when:**

 * Accessing APIs that are returning @scala[`Future`]@java[`CompletionStage`] from an actor, such as a database or
   an external service
 * The actor needs to continue processing when the @scala[`Future`]@java[`CompletionStage`] has completed
 * Keep context from the original request and use that when the @scala[`Future`]@java[`CompletionStage`] has completed,
   for example an `replyTo` actor reference
 
**Problems:**

 * Boilerplate of adding wrapper messages for the results

## Per session child Actor

In some cases a complete response to a request can only be created and sent back after collecting multiple answers from other actors. For these kinds of interaction it can be good to delegate the work to a per "session" child actor. The child could also contain arbitrary logic to implement retrying, failing on timeout, tail chopping, progress inspection etc.

Note that this is essentially how `ask` is implemented, if all you need is a single response with a timeout it is better to use `ask`.

The child is created with the context it needs to do the work, including an @apidoc[akka.actor.typed.ActorRef] that it can respond to. When the complete result is there the child responds with the result and stops itself.

As the protocol of the session actor is not a public API but rather an implementation detail of the parent actor, it may not always make sense to have an explicit protocol and adapt the messages of the actors that the session actor interacts with. For this use case it is possible to express that the actor can receive any message (@scala[@scaladoc[Any](scala.Any)]@java[@javadoc[Object](java.lang.Object)]).

**Example:**

![per-session-child.png](./images/per-session-child.png)

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #per-session-child }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #per-session-child }

In an actual session child you would likely want to include some form of timeout as well (see @ref:[scheduling messages to self](#scheduling-messages-to-self)).

**Useful when:**

 * A single incoming request should result in multiple interactions with other actors before a result can be built,
   for example aggregation of several results
 * You need to handle acknowledgement and retry messages for at-least-once delivery

**Problems:**

 * Children have life cycles that must be managed to not create a resource leak, it can be easy to miss a scenario where the session actor is not stopped
 * It increases complexity, since each such child can execute concurrently with other children and the parent

## General purpose response aggregator

This is similar to above @ref:[Per session child Actor](#per-session-child-actor) pattern. Sometimes you might
end up repeating the same way of aggregating replies and want to extract that to a reusable actor.

There are many variations of this pattern and that is the reason this is provided as a documentation
example rather than a built in @apidoc[akka.actor.typed.Behavior] in Akka. It is intended to be adjusted to your specific needs.

**Example:**

![aggregator.png](./images/aggregator.png)

This example is an aggregator of expected number of replies.
Requests for quotes are sent with the given `sendRequests` function to the two hotel actors, which both speak
different protocols. When both expected replies have been collected they are aggregated with the given `aggregateReplies`
function and sent back to the `replyTo`. If replies don't arrive within the `timeout` the replies so far are
aggregated and sent back to the `replyTo`.

Scala
:  @@snip [AggregatorSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/AggregatorSpec.scala) { #usage }

Java
:  @@snip [AggregatorTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/AggregatorTest.java) { #usage }


The implementation of the `Aggregator`:

Scala
:  @@snip [Aggregator.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/Aggregator.scala) { #behavior }

Java
:  @@snip [Aggregator.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/Aggregator.java) { #behavior }

**Useful when:**

 * Aggregating replies are performed in the same way at multiple places and should be extracted to a more general
   purpose actor.
 * A single incoming request should result in multiple interactions with other actors before a result can be built,
   for example aggregation of several results
 * You need to handle acknowledgement and retry messages for at-least-once delivery

**Problems:**

 * Message protocols with generic types are difficult since the generic types are erased in runtime
 * Children have life cycles that must be managed to not create a resource leak, it can be easy to miss a scenario where the session actor is not stopped
 * It increases complexity, since each such child can execute concurrently with other children and the parent

## Latency tail chopping

This is a variation of above @ref:[General purpose response aggregator](#general-purpose-response-aggregator) pattern.

The goal of this algorithm is to decrease tail latencies ("chop off the tail latency") in situations
where multiple destination actors can perform the same piece of work, and where an actor may occasionally respond
more slowly than expected. In this case, sending the same work request (also known as a "backup request")
to another actor results in decreased response time - because it's less probable that multiple actors
are under heavy load simultaneously. This technique is explained in depth in Jeff Dean's presentation on
[Achieving Rapid Response Times in Large Online Services](https://static.googleusercontent.com/media/research.google.com/en//people/jeff/Berkeley-Latency-Mar2012.pdf).

There are many variations of this pattern and that is the reason this is provided as a documentation
example rather than a built in @apidoc[akka.actor.typed.Behavior] in Akka. It is intended to be adjusted to your specific needs.

**Example:**

![tail-chopping.png](./images/tail-chopping.png)

Scala
:  @@snip [TailChopping.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/TailChopping.scala) { #behavior }

Java
:  @@snip [TailChopping.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/TailChopping.java) { #behavior }

**Useful when:**

 * Reducing higher latency percentiles and variations of latency are important
 * The "work" can be done more than once with the same result, e.g. a request to retrieve information

**Problems:**

 * Increased load since more messages are sent and "work" is performed more than once
 * Can't be used when the "work" is not idempotent and must only be performed once
 * Message protocols with generic types are difficult since the generic types are erased in runtime
 * Children have life cycles that must be managed to not create a resource leak, it can be easy to miss a scenario where the session actor is not stopped


<a id="typed-scheduling"></a>
## Scheduling messages to self

The following example demonstrates how to use timers to schedule messages to an actor. 

**Example:**

![timer.png](./images/timer.png)

The `Buncher` actor buffers a burst of incoming messages and delivers them as a batch after a timeout or when the number of batched messages exceeds a maximum size.

Scala
:  @@snip [InteractionPatternsSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/InteractionPatternsSpec.scala) { #timer }

Java
:  @@snip [InteractionPatternsTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/InteractionPatternsTest.java) { #timer }

There are a few things worth noting here:

* To get access to the timers you start with @apidoc[Behaviors.withTimers](typed.*.Behaviors$) {scala="#withTimers[T](factory:akka.actor.typed.scaladsl.TimerScheduler[T]=%3eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#withTimers(akka.japi.function.Function)"} that will pass a @apidoc[akka.actor.typed.*.TimerScheduler] instance to the function. 
This can be used with any type of @apidoc[akka.actor.typed.Behavior], including @apidoc[receive](typed.*.Behaviors$) {scala="#receive[T](onMessage:(akka.actor.typed.scaladsl.ActorContext[T],T)=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.scaladsl.Behaviors.Receive[T]" java="#receive(akka.japi.function.Function2,akka.japi.function.Function2)"}, @apidoc[receiveMessage](typed.*.Behaviors$) {scala="#receiveMessage[T](onMessage:T=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.scaladsl.Behaviors.Receive[T]" java="#receiveMessage(akka.japi.Function)"}, but also @apidoc[setup](typed.*.Behaviors$) {scala="#setup[T](factory:akka.actor.typed.scaladsl.ActorContext[T]=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#setup(akka.japi.function.Function)"} or any other behavior.
* Each timer has a key and if a new timer with the same key is started, the previous is cancelled. It is guaranteed that a message from the previous timer is not received, even if it was already enqueued in the mailbox when the new timer was started.
* Both periodic and single message timers are supported. 
* The `TimerScheduler` is mutable in itself, because it performs and manages the side effects of registering the scheduled tasks.
* The `TimerScheduler` is bound to the lifecycle of the actor that owns it and is cancelled automatically when the actor is stopped.
* `Behaviors.withTimers` can also be used inside @apidoc[Behaviors.supervise](typed.*.Behaviors$) {scala="#supervise[T](wrapped:akka.actor.typed.Behavior[T]):akka.actor.typed.scaladsl.Behaviors.Supervise[T]" java="#supervise(akka.actor.typed.Behavior)"} and it will automatically cancel the started timers correctly when the actor is restarted, so that the new incarnation will not receive scheduled messages from a previous incarnation.

### Schedule periodically

Scheduling of recurring messages can have two different characteristics:

* fixed-delay - The delay between sending subsequent messages will always be (at least) the given `delay`.
  Use @apidoc[startTimerWithFixedDelay](akka.actor.TimerScheduler) {scala="#startTimerWithFixedDelay(key:Any,msg:Any,initialDelay:scala.concurrent.duration.FiniteDuration,delay:scala.concurrent.duration.FiniteDuration):Unit" java="#startTimerWithFixedDelay(java.lang.Object,java.lang.Object,java.time.Duration,java.time.Duration)"}.
* fixed-rate - The frequency of execution over time will meet the given `interval`. Use @apidoc[startTimerAtFixedRate](akka.actor.TimerScheduler) {scala="#startTimerAtFixedRate(key:Any,msg:Any,interval:scala.concurrent.duration.FiniteDuration):Unit" java="#startTimerAtFixedRate(java.lang.Object,java.lang.Object,java.time.Duration,java.time.Duration)"}.

If you are uncertain of which one to use you should pick `startTimerWithFixedDelay`.

When using **fixed-delay** it will not compensate the delay between messages if the scheduling is delayed longer
than specified for some reason. The delay between sending subsequent messages will always be (at least) the given
`delay`. In the long run, the frequency of messages will generally be slightly lower than the reciprocal of the
specified `delay`.

Fixed-delay execution is appropriate for recurring activities that require "smoothness." In other words,
it is appropriate for activities where it is more important to keep the frequency accurate in the short run
than in the long run.

When using **fixed-rate** it will compensate the delay for a subsequent task if the previous messages were delayed
too long. In such cases, the actual sending interval will differ from the interval passed to the `scheduleAtFixedRate`
method.

If the tasks are delayed longer than the `interval`, the subsequent message will be sent immediately after the
prior one. This also has the consequence that after long garbage collection pauses or other reasons when the JVM
was suspended all "missed" tasks will execute when the process wakes up again. For example, `scheduleAtFixedRate`
with an interval of 1 second and the process is suspended for 30 seconds will result in 30 messages being sent
in rapid succession to catch up. In the long run, the frequency of execution will be exactly the reciprocal of
the specified `interval`.

Fixed-rate execution is appropriate for recurring activities that are sensitive to absolute time
or where the total time to perform a fixed number of executions is important, such as a countdown
timer that ticks once every second for ten seconds.

@@@ warning

`scheduleAtFixedRate` can result in bursts of scheduled messages after long garbage collection pauses,
which may in worst case cause undesired load on the system. `scheduleWithFixedDelay` is often preferred.

@@@

## Responding to a sharded actor

When @ref:[Akka Cluster](cluster.md) is used to @ref:[shard actors](cluster-sharding.md) you need to
take into account that an actor may move or get passivated.

The normal pattern for expecting a reply is to include an @apidoc[akka.actor.typed.ActorRef] in the message, typically a message adapter. This can be used
for a sharded actor but if @scala[@scaladoc[ctx.self](akka.actor.typed.scaladsl.ActorContext#self:akka.actor.typed.ActorRef[T])]@java[@javadoc[ctx.getSelf()](akka.actor.typed.javadsl.ActorContext#getSelf())] is sent and the sharded actor is moved or passivated then the reply
will sent to dead letters.

An alternative is to send the `entityId` in the message and have the reply sent via sharding.

**Example:**

![sharded-response.png](./images/sharded-response.png)

Scala
:  @@snip [sharded.response](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #sharded-response }

Java
:  @@snip [sharded.response](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingReplyCompileOnlyTest.java) { #sharded-response }

A disadvantage is that a message adapter can't be used so the response has to be in the protocol of the actor being responded to. Additionally the @apidoc[typed.*.EntityTypeKey]
could be included in the message if it is not known statically.

As an "alternative to the alternative", an @apidoc[typed.*.EntityRef] can be included in the messages.  The `EntityRef` transparently wraps messages in a @apidoc[typed.ShardingEnvelope] and 
sends them via sharding.  If the target sharded entity has been passivated, it will be delivered to a new incarnation of that entity; if the target sharded entity
has been moved to a different cluster node, it will be routed to that new node.  If using this approach, be aware that at this time, @ref:[a custom serializer is required](cluster-sharding.md#a-note-about-entityref-and-serialization).

As with directly including the `entityId` and `EntityTypeKey` in the message, `EntityRef`s do not support message adaptation: the response has to be in the protocol
of the entity being responded to.

In some cases, it may be useful to define messages with a @apidoc[akka.actor.typed.RecipientRef] which is a common supertype of @apidoc[typed.ActorRef] and `EntityRef`.  At this time,
serializing a `RecipientRef` requires a custom serializer.
