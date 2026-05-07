# Basics and working with Flows

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

## Introduction

## Core concepts

Akka Streams is a library to process and transfer a sequence of elements using bounded buffer space. This
latter property is what we refer to as *boundedness*, and it is the defining feature of Akka Streams. Translated to
everyday terms, it is possible to express a chain (or as we see later, graphs) of processing entities. Each of these
entities executes independently (and possibly concurrently) from the others while only buffering a limited number
of elements at any given time. This property of bounded buffers is one of the differences from the actor model, where each actor usually has
an unbounded, or a bounded, but dropping mailbox. Akka Stream processing entities have bounded "mailboxes" that
do not drop.

Before we move on, let's define some basic terminology which will be used throughout the entire documentation:

Stream
: An active process that involves moving and transforming data.

Element
: An element is the processing unit of streams. All operations transform and transfer elements from upstream to
downstream. Buffer sizes are always expressed as number of elements independently from the actual size of the elements.

Back-pressure
: A means of flow-control, a way for consumers of data to notify a producer about their current availability, effectively
slowing down the upstream producer to match their consumption speeds.
In the context of Akka Streams back-pressure is always understood as *non-blocking* and *asynchronous*.

Non-Blocking
: Means that a certain operation does not hinder the progress of the calling thread, even if it takes a long time to
finish the requested operation.

Graph
: A description of a stream processing topology, defining the pathways through which elements shall flow when the stream
is running.

Operator
: The common name for all building blocks that build up a Graph.
Examples of operators are `map()`, `filter()`, custom ones extending @ref[`GraphStage`s](stream-customize.md) and graph
junctions like `Merge` or `Broadcast`. For the full list of built-in operators see the @ref:[operator index](operators/index.md)


When we talk about *asynchronous, non-blocking backpressure*, we mean that the operators available in Akka
Streams will not use blocking calls but asynchronous message passing to exchange messages between each other.
This way they can slow down a fast producer without blocking its thread. This is a thread-pool friendly
design, since entities that need to wait (a fast producer waiting on a slow consumer) will not block the thread but
can hand it back for further use to an underlying thread-pool.

## Defining and running streams

Linear processing pipelines can be expressed in Akka Streams using the following core abstractions:

Source
: An operator with *exactly one output*, emitting data elements whenever downstream operators are
ready to receive them.

Sink
: An operator with *exactly one input*, requesting and accepting data elements, possibly slowing down the upstream
producer of elements.

Flow
: An operator which has *exactly one input and output*, which connects its upstream and downstream by
transforming the data elements flowing through it.

RunnableGraph
: A Flow that has both ends "attached" to a Source and Sink respectively, and is ready to be `run()`.


It is possible to attach a @apidoc[stream.*.Flow] to a @apidoc[stream.*.Source] resulting in a composite source, and it is also possible to prepend
a `Flow` to a @apidoc[stream.*.Sink] to get a new sink. After a stream is properly constructed by having both a source and a sink,
it will be represented by the @apidoc[stream.*.RunnableGraph] type, indicating that it is ready to be executed.

It is important to remember that even after constructing the `RunnableGraph` by connecting all the source, sink and
different operators, no data will flow through it until it is materialized. Materialization is the process of
allocating all resources needed to run the computation described by a Graph (in Akka Streams this will often involve
starting up Actors). Thanks to Flows being a description of the processing pipeline they are *immutable,
thread-safe, and freely shareable*, which means that it is for example safe to share and send them between actors, to have
one actor prepare the work, and then have it be materialized at some completely different place in the code.

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #materialization-in-steps }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #materialization-in-steps }

@@@ div { .group-scala }

After running (materializing) the `RunnableGraph[T]` we get back the materialized value of type T. Every stream
operator can produce a materialized value, and it is the responsibility of the user to combine them to a new type.
In the above example, we used @scaladoc[toMat](akka.stream.scaladsl.Source#toMat[Mat2,Mat3](sink:akka.stream.Graph[akka.stream.SinkShape[Out],Mat2])(combine:(Mat,Mat2)=%3EMat3):akka.stream.scaladsl.RunnableGraph[Mat3]) to indicate that we want to transform the materialized value of the source and
sink, and we used the convenience function @scaladoc[Keep.right](akka.stream.scaladsl.Keep$#right[L,R]:(L,R)=%3ER) to say that we are only interested in the materialized value
of the sink.

In our example, the @scaladoc[Sink.fold](akka.stream.scaladsl.Sink$#fold[U,T](zero:U)(f:(U,T)=&gt;U):akka.stream.scaladsl.Sink[T,scala.concurrent.Future[U]]) materializes a value of type @scaladoc[Future](scala.concurrent.Future) which will represent the result
of the folding process over the stream.  In general, a stream can expose multiple materialized values,
but it is quite common to be interested in only the value of the Source or the Sink in the stream. For this reason
there is a convenience method called `runWith()` available for @scaladoc[Sink](akka.stream.scaladsl.Sink), @scaladoc[Source](akka.stream.scaladsl.Source) or @scaladoc[Flow](akka.stream.scaladsl.Flow) requiring, respectively,
a supplied `Source` (in order to run a `Sink`), a `Sink` (in order to run a `Source`) or
both a `Source` and a `Sink` (in order to run a `Flow`, since it has neither attached yet).

@@@

@@@ div { .group-java }

After running (materializing) the @javadoc[RunnableGraph](akka.stream.javadsl.RunnableGraph) we get a special container object, the `MaterializedMap`. Both
sources and sinks are able to put specific objects into this map. Whether they put something in or not is implementation
dependent. 

For example, a @javadoc[Sink.fold](akka.stream.javadsl.Sink$#fold(java.lang.Object,akka.japi.function.Function2)) will make a @javadoc[CompletionStage](java.util.concurrent.CompletionStage) available in this map which will represent the result
of the folding process over the stream.  In general, a stream can expose multiple materialized values,
but it is quite common to be interested in only the value of the Source or the Sink in the stream. For this reason
there is a convenience method called `runWith()` available for @javadoc[Sink](akka.stream.javadsl.Sink), @javadoc[Source](akka.stream.javadsl.Source) or @javadoc[Flow](akka.stream.javadsl.Flow) requiring, respectively,
a supplied `Source` (in order to run a `Sink`), a `Sink` (in order to run a `Source`) or
both a `Source` and a `Sink` (in order to run a `Flow`, since it has neither attached yet).
@@@

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #materialization-runWith }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #materialization-runWith }

It is worth pointing out that since operators are *immutable*, connecting them returns a new operator,
instead of modifying the existing instance, so while constructing long flows, remember to assign the new value to a variable or run it:

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #source-immutable }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #source-immutable }

@@@ note

By default, Akka Streams elements support **exactly one** downstream operator.
Making fan-out (supporting multiple downstream operators) an explicit opt-in feature allows default stream elements to
be less complex and more efficient. Also, it allows for greater flexibility on *how exactly* to handle the multicast scenarios,
by providing named fan-out elements such as broadcast (signals all down-stream elements) or balance (signals one of available down-stream elements).

@@@

In the above example we used the `runWith` method, which both materializes the stream and returns the materialized value
of the given sink or source.

Since a stream can be materialized multiple times, the @scala[materialized value will also be calculated anew] @java[`MaterializedMap` returned is different] for each such
materialization, usually leading to different values being returned each time.
In the example below, we create two running materialized instances of the stream that we described in the `runnable`
variable. Both materializations give us a different @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] from the map even though we used the same `sink`
to refer to the future:

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #stream-reuse }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #stream-reuse }

### Defining sources, sinks and flows

The objects @apidoc[stream.*.Source] and @apidoc[stream.*.Sink] define various ways to create sources and sinks of elements. The following
examples show some of the most useful constructs (refer to the API documentation for more details):

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #source-sink }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #source-sink }

There are various ways to wire up different parts of a stream, the following examples show some of the available options:

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #flow-connecting }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #flow-connecting }

### Illegal stream elements

In accordance to the Reactive Streams specification ([Rule 2.13](https://github.com/reactive-streams/reactive-streams-jvm#2.13))
Akka Streams do not allow `null` to be passed through the stream as an element. In case you want to model the concept
of absence of a value we recommend using @scala[@scaladoc[scala.Option](scala.Option) or @scaladoc[scala.util.Either](scala.util.Either)]@java[@javadoc[java.util.Optional](java.util.Optional) which is available since Java 8].

## Back-pressure explained

Akka Streams implement an asynchronous non-blocking back-pressure protocol standardised by the [Reactive Streams](https://www.reactive-streams.org/)
specification, which Akka is a founding member of.

The user of the library does not have to write any explicit back-pressure handling code — it is built in
and dealt with automatically by all of the provided Akka Streams operators. It is possible however to add
explicit buffer operators with overflow strategies that can influence the behavior of the stream. This is especially important
in complex processing graphs which may even contain loops (which *must* be treated with very special
care, as explained in @ref:[Graph cycles, liveness and deadlocks](stream-graphs.md#graph-cycles)).

The back pressure protocol is defined in terms of the number of elements a downstream `Subscriber` is able to receive
and buffer, referred to as `demand`.
The source of data, referred to as `Publisher` in Reactive Streams terminology and implemented as @apidoc[stream.*.Source] in Akka
Streams, guarantees that it will never emit more elements than the received total demand for any given `Subscriber`.

@@@ note

The Reactive Streams specification defines its protocol in terms of `Publisher` and `Subscriber`.
These types are **not** meant to be user facing API, instead they serve as the low-level building blocks for
different Reactive Streams implementations.

Akka Streams implements these concepts as @apidoc[stream.*.Source], @apidoc[Flow](stream.*.Flow) (referred to as `Processor` in Reactive Streams)
and @apidoc[stream.*.Sink] without exposing the Reactive Streams interfaces directly.
If you need to integrate with other Reactive Stream libraries, read @ref:[Integrating with Reactive Streams](reactive-streams-interop.md).

@@@

The mode in which Reactive Streams back-pressure works can be colloquially described as "dynamic push / pull mode",
since it will switch between push and pull based back-pressure models depending on the downstream being able to cope
with the upstream production rate or not.

To illustrate this further let us consider both problem situations and how the back-pressure protocol handles them:

### Slow Publisher, fast Subscriber

This is the happy case – we do not need to slow down the Publisher in this case. However signalling rates are
rarely constant and could change at any point in time, suddenly ending up in a situation where the Subscriber is now
slower than the Publisher. In order to safeguard from these situations, the back-pressure protocol must still be enabled
during such situations, however we do not want to pay a high penalty for this safety net being enabled.

The Reactive Streams protocol solves this by asynchronously signalling from the Subscriber to the Publisher
@scala[`Request(n:Int)`] @java[`Request(int n)`] signals. The protocol guarantees that the Publisher will never signal *more* elements than the
signalled demand. Since the Subscriber however is currently faster, it will be signalling these Request messages at a higher
rate (and possibly also batching together the demand - requesting multiple elements in one Request signal). This means
that the Publisher should not ever have to wait (be back-pressured) with publishing its incoming elements.

As we can see, in this scenario we effectively operate in so called push-mode since the Publisher can continue producing
elements as fast as it can, since the pending demand will be recovered just-in-time while it is emitting elements.

### Fast Publisher, slow Subscriber

This is the case when back-pressuring the `Publisher` is required, because the `Subscriber` is not able to cope with
the rate at which its upstream would like to emit data elements.

Since the `Publisher` is not allowed to signal more elements than the pending demand signalled by the `Subscriber`,
it will have to abide to this back-pressure by applying one of the below strategies:

 * not generate elements, if it is able to control their production rate,
 * try buffering the elements in a *bounded* manner until more demand is signalled,
 * drop elements until more demand is signalled,
 * tear down the stream if unable to apply any of the above strategies.

As we can see, this scenario effectively means that the `Subscriber` will *pull* the elements from the Publisher –
this mode of operation is referred to as pull-based back-pressure.

## Stream Materialization

When constructing flows and graphs in Akka Streams think of them as preparing a blueprint, an execution plan.
Stream materialization is the process of taking a stream description (@apidoc[stream.*.RunnableGraph]) and allocating all the necessary resources
it needs in order to run. In the case of Akka Streams this often means starting up Actors which power the processing,
but is not restricted to that—it could also mean opening files or socket connections etc.—depending on what the stream needs.

Materialization is triggered at so called "terminal operations". Most notably this includes the various forms of the `run()`
and `runWith()` methods defined on @apidoc[stream.*.Source] and @apidoc[stream.*.Flow] elements as well as a small number of special syntactic sugars for running with
well-known sinks, such as @scala[`runForeach(el => ...)`]@java[`runForeach(el -> ...)`]
(being an alias to @scala[`runWith(Sink.foreach(el => ...))`]@java[`runWith(Sink.foreach(el -> ...))`]).

Materialization is performed synchronously on the materializing thread by an @apidoc[actor.ActorSystem] global @apidoc[stream.Materializer].
The actual stream processing is handled by actors started up during the streams materialization,
which will be running on the thread pools they have been configured to run on.  By default, the thread pool used is the
dispatcher set on the `ActorSystem` config, but it is possible set other thread pools by providing @apidoc[stream.Attributes] to

  * the stream to be materialized
  * creating a custom instance of @apidoc[stream.Materializer] with `defaultAttributes`

@@@ note

Reusing *instances* of linear computation operators (Source, Sink, Flow) inside composite Graphs is legal,
yet will materialize that operator multiple times.

@@@

### Operator Fusion

By default, Akka Streams will fuse the stream operators. This means that the processing steps of a flow or
stream can be executed within the same Actor and has two consequences:

 * passing elements from one operator to the next is a lot faster between fused
operators due to avoiding the asynchronous messaging overhead
 * fused stream operators do not run in parallel to each other, meaning that
only up to one CPU core is used for each fused part

To allow for parallel processing you will have to insert asynchronous boundaries manually into your flows and
operators by way of adding @apidoc[Attributes.asyncBoundary](stream.Attributes$) {scala="#asyncBoundary:akka.stream.Attributes" java="#asyncBoundary()"} using the method `async` on @apidoc[stream.*.Source], @apidoc[stream.*.Sink] and @apidoc[stream.*.Flow]
to operators that shall communicate with the downstream of the graph in an asynchronous fashion.

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #flow-async }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #flow-async }

In this example we create two regions within the flow which will be executed in one Actor each—assuming that adding
and multiplying integers is an extremely costly operation this will lead to a performance gain since two CPUs can
work on the tasks in parallel. It is important to note that asynchronous boundaries are not singular places within a
flow where elements are passed asynchronously (as in other streaming libraries), but instead attributes always work
by adding information to the flow graph that has been constructed up to this point:

![asyncBoundary.png](../images/asyncBoundary.png)

This means that everything that is inside the red bubble will be executed by one actor and everything outside of it
by another. This scheme can be applied successively, always having one such boundary enclose the previous ones plus all
operators that have been added since then.

@@@ warning

Without fusing (i.e. up to version 2.0-M2) each stream operator had an implicit input buffer
that holds a few elements for efficiency reasons. If your flow graphs contain cycles then these buffers
may have been crucial in order to avoid deadlocks. With fusing these implicit buffers are no longer
there, data elements are passed without buffering between fused operators. In those cases where buffering
is needed in order to allow the stream to run at all, you will have to insert explicit buffers with the
`.buffer()` operator—typically a buffer of size 2 is enough to allow a feedback loop to function.

@@@

<a id="flow-combine-mat"></a>
### Combining materialized values

Since every operator in Akka Streams can provide a materialized value after being materialized, it is necessary
to somehow express how these values should be composed to a final value when we plug these operators together. For this,
many operator methods have variants that take an additional argument, a function, that will be used to combine the
resulting values. Some examples of using these combiners are illustrated in the example below.

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #flow-mat-combine }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #flow-mat-combine }

@@@ note

In Graphs it is possible to access the materialized value from inside the stream. For details see @ref:[Accessing the materialized value inside the Graph](stream-graphs.md#graph-matvalue).

@@@

### Source pre-materialization

There are situations in which you require a @apidoc[stream.*.Source] materialized value **before** the `Source` gets hooked up to the rest of the graph.
This is particularly useful in the case of "materialized value powered" `Source`s, like `Source.queue`, `Source.actorRef` or `Source.maybe`.

By using the @apidoc[preMaterialize](stream.*.Source) {scala="#preMaterialize()(implicitmaterializer:akka.stream.Materializer):(Mat,Source.this.ReprMat[Out,akka.NotUsed])" java="#preMaterialize(akka.stream.Materializer)"} operator on a `Source`, you can obtain its materialized value and another `Source`. The latter can be used
to consume messages from the original `Source`. Note that this can be materialized multiple times.

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #source-prematerialization }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #source-prematerialization }

## Stream ordering

In Akka Streams almost all computation operators *preserve input order* of elements. This means that if inputs `{IA1,IA2,...,IAn}`
"cause" outputs `{OA1,OA2,...,OAk}` and inputs `{IB1,IB2,...,IBm}` "cause" outputs `{OB1,OB2,...,OBl}` and all of
`IAi` happened before all `IBi` then `OAi` happens before `OBi`.

This property is even upheld by async operations such as @apidoc[mapAsync](stream.*.Source) { scala="#mapAsync[T](parallelism:Int)(f:Out=%3Escala.concurrent.Future[T]):FlowOps.this.Repr[T]" java="#mapAsync(int,akka.japi.function.Function)" }, however an unordered version exists
called @apidoc[mapAsyncUnordered](stream.*.Source) { scala="#mapAsyncUnordered[T](parallelism:Int)(f:Out=&gt;scala.concurrent.Future[T]):FlowOps.this.Repr[T]" java="#mapAsyncUnordered(int,akka.japi.function.Function)" } which does not preserve this ordering.

However, in the case of Junctions which handle multiple input streams (e.g. `Merge`) the output order is,
in general, *not defined* for elements arriving on different input ports. That is a merge-like operation may emit `Ai`
before emitting `Bi`, and it is up to its internal logic to decide the order of emitted elements. Specialized elements
such as `Zip` however *do guarantee* their outputs order, as each output element depends on all upstream elements having
been signalled already – thus the ordering in the case of zipping is defined by this property.

If you find yourself in need of fine grained control over order of emitted elements in fan-in
scenarios consider using `MergePreferred`, `MergePrioritized` or @ref[`GraphStage`](stream-customize.md) – which gives you full control over how the
merge is performed.

## Actor Materializer Lifecycle

The @apidoc[akka.stream.Materializer] is a component that is responsible for turning the stream blueprint into a running stream
and emitting the "materialized value". An @apidoc[akka.actor.ActorSystem] wide `Materializer` is provided by the Akka `Extension` 
@apidoc[SystemMaterializer] by @scala[having an implicit `ActorSystem` in scope]@java[passing the `ActorSystem` to the 
various `run` methods] this way there is no need to worry about the `Materializer` unless there are special requirements.

One use case that may require a custom instance of `Materializer` is when all streams materialized in an actor should be tied to the Actor lifecycle and stop if the Actor stops or crashes. 

An important aspect of working with streams and actors is understanding a `Materializer`'s life-cycle.
The materializer is bound to the lifecycle of the @apidoc[akka.actor.ActorRefFactory] it is created from, which in practice will
be either an @apidoc[akka.actor.ActorSystem] or @apidoc[ActorContext](akka.actor.ActorContext) (when the materializer is created within an @apidoc[akka.actor.Actor]). 

Tying it to the `ActorSystem` should be replaced with using the system materializer from Akka 2.6 and on.

When run by the system materializer the streams will run until the `ActorSystem` is shut down. When the materializer is shut down
*before* the streams have run to completion, they will be terminated abruptly. This is a little different than the
usual way to terminate streams, which is by cancelling/completing them. The stream lifecycles are bound to the materializer
like this to prevent leaks, and in normal operations you should not rely on the mechanism and rather use @apidoc[akka.stream.KillSwitch] or
normal completion signals to manage the lifecycles of your streams.  

If we look at the following example, where we create the `Materializer` within an `Actor`:

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #materializer-from-actor-context }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #materializer-from-actor-context }

In the above example we used the @apidoc[akka.actor.ActorContext] to create the materializer. This binds its lifecycle to the surrounding @apidoc[akka.actor.Actor]. In other words, while the stream we started there would under normal circumstances run forever, if we stop the Actor it would terminate the stream as well. We have *bound the stream's lifecycle to the surrounding actor's lifecycle*.
This is a very useful technique if the stream is closely related to the actor, e.g. when the actor represents a user or other entity, that we continuously query using the created stream -- and it would not make sense to keep the stream alive when the actor has terminated already. The streams termination will be signalled by an "Abrupt termination exception" signaled by the stream.

You may also cause a `Materializer` to shut down by explicitly calling @apidoc[shutdown()](akka.stream.Materializer) {scala="#shutdown():Unit" java="#shutdown()"} on it, resulting in abruptly terminating all of the streams it has been running then. 

Sometimes, however, you may want to explicitly create a stream that will out-last the actor's life.
For example, you are using an Akka stream to push some large stream of data to an external service.
You may want to eagerly stop the Actor since it has performed all of its duties already:

Scala
:   @@snip [FlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowDocSpec.scala) { #materializer-from-system-in-actor }

Java
:   @@snip [FlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowDocTest.java) { #materializer-from-system-in-actor }

In the above example we pass in a materializer to the Actor, which results in binding its lifecycle to the entire @apidoc[akka.actor.ActorSystem] rather than the single enclosing actor. This can be useful if you want to share a materializer or group streams into specific materializers,
for example because of the materializer's settings etc.

@@@ warning

Do not create new actor materializers inside actors by passing the @scala[@scaladoc[context.system](akka.actor.ActorContext#system:akka.actor.ActorSystem)]@java[@javadoc[context.system()](akka.actor.ActorContext#system())] to it. 
This will cause a new @apidoc[akka.stream.Materializer] to be created and potentially leaked (unless you shut it down explicitly) for each such actor.
It is instead recommended to either pass-in the Materializer or create one using the actor's `context`.

@@@
