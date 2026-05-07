# Streams Quickstart Guide

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

@@@ note

Both the Java and Scala DSLs of Akka Streams are bundled in the same JAR. For a smooth development experience, when using an IDE such as Eclipse or IntelliJ, you can disable the auto-importer from suggesting `javadsl` imports when working in Scala,
or viceversa. See @ref:[IDE Tips](../additional/ide.md). 
@@@

## First steps

A stream usually begins at a source, so this is also how we start an Akka
Stream. Before we create one, we import the full complement of streaming tools:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #stream-imports }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #stream-imports }

If you want to execute the code samples while you read through the quick start guide, you will also need the following imports:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #other-imports }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #other-imports }

And @scala[an object]@java[a class] to start an Akka @apidoc[akka.actor.ActorSystem] and hold your code @scala[. Making the `ActorSystem`
implicit makes it available to the streams without manually passing it when running them]:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #main-app }

Java
:   @@snip [Main.java](/akka-docs/src/test/java/jdocs/stream/Main.java) { #main-app }

Now we will start with a rather simple source, emitting the integers 1 to 100:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #create-source }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #create-source }

The @apidoc[akka.stream.*.Source] type is parameterized with two types: the first one is the
type of element that this source emits and the second one, the "materialized value", allows
running the source to produce some auxiliary value (e.g. a network source may
provide information about the bound port or the peer’s address). Where no
auxiliary information is produced, the type @apidoc[akka.NotUsed] is used. A
simple range of integers falls into this category - running our stream produces
a `NotUsed`.

Having created this source means that we have a description of how to emit the
first 100 natural numbers, but this source is not yet active. In order to get
those numbers out we have to run it:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #run-source }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #run-source }

This line will complement the source with a consumer function—in this example
we print out the numbers to the console—and pass this little stream
setup to an Actor that runs it. This activation is signaled by having “run” be
part of the method name; there are other methods that run Akka Streams, and
they all follow this pattern.

When running this @scala[source in a `scala.App`]@java[program] you might notice it does not
terminate, because the @apidoc[akka.actor.ActorSystem] is never terminated. Luckily
@apidoc[runForeach](akka.stream.*.Source) {scala="#runForeach(f:Out=%3EUnit)(implicitmaterializer:akka.stream.Materializer):scala.concurrent.Future[akka.Done]" java="#runForeach(akka.japi.function.Procedure,akka.stream.Materializer)"} returns a @scala[@scaladoc[Future](scala.concurrent.Future)[@apidoc[akka.Done]]]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)<@apidoc[akka.Done]>] which resolves when the stream finishes:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #run-source-and-terminate }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #run-source-and-terminate }

The nice thing about Akka Streams is that the `Source` is a
description of what you want to run, and like an architect’s blueprint it can
be reused, incorporated into a larger design. We may choose to transform the
source of integers and write it to a file instead:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #transform-source }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #transform-source }

First we use the @apidoc[scan](akka.stream.*.Source) {scala="#scan[T](zero:T)(f:(T,Out)=%3ET):FlowOps.this.Repr[T]" java="#scan(T,akka.japi.function.Function2)"} operator to run a computation over the whole
stream: starting with the number 1 (@scala[`BigInt(1)`]@java[`BigInteger.ONE`]) we multiply by each of
the incoming numbers, one after the other; the scan operation emits the initial
value and then every calculation result. This yields the series of factorial
numbers which we stash away as a `Source` for later reuse—it is
important to keep in mind that nothing is actually computed yet, this is a
description of what we want to have computed once we run the stream. Then we
convert the resulting series of numbers into a stream of @apidoc[akka.util.ByteString]
objects describing lines in a text file. This stream is then run by attaching a
file as the receiver of the data. In the terminology of Akka Streams this is
called a @apidoc[akka.stream.*.Sink]. @apidoc[akka.stream.IOResult] is a type that IO operations return in
Akka Streams in order to tell you how many bytes or elements were consumed and
whether the stream terminated normally or exceptionally.

## Reusable Pieces

One of the nice parts of Akka Streams—and something that other stream libraries
do not offer—is that not only sources can be reused like blueprints, all other
elements can be as well. We can take the file-writing @apidoc[akka.stream.*.Sink], prepend
the processing steps necessary to get the @apidoc[akka.util.ByteString] elements from
incoming strings and package that up as a reusable piece as well. Since the
language for writing these streams always flows from left to right (just like
plain English), we need a starting point that is like a source but with an
“open” input. In Akka Streams this is called a @apidoc[akka.stream.*.Flow]:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #transform-sink }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #transform-sink }

Starting from a flow of strings we convert each to `ByteString` and then
feed to the already known file-writing `Sink`. The resulting blueprint
is a @scala[`Sink[String, Future[IOResult]]`]@java[`Sink<String, CompletionStage<IOResult>>`], which means that it
accepts strings as its input and when materialized it will create auxiliary
information of type @scala[@scaladoc[Future](scala.concurrent.Future)[@apidoc[akka.stream.IOResult]]]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)<@apidoc[akka.stream.IOResult]>] (when chaining operations on
a `Source` or `Flow` the type of the auxiliary information—called
the “materialized value”—is given by the leftmost starting point; since we want
to retain what the @apidoc[FileIO.toPath](akka.stream.*.FileIO$) {scala="#toPath(f:java.nio.file.Path,options:Set[java.nio.file.OpenOption],startPosition:Long):akka.stream.scaladsl.Sink[akka.util.ByteString,scala.concurrent.Future[akka.stream.IOResult]]" java="#toPath(java.nio.file.Path)"} sink has to offer, we need to say
@scala[@scaladoc[Keep.right](akka.stream.scaladsl.Keep$#right[L,R]:(L,R)=%3ER)]@java[@javadoc[Keep.right()](akka.stream.javadsl.Keep$#right())].

We can use the new and shiny @apidoc[akka.stream.*.Sink] we just created by
attaching it to our `factorials` source—after a small adaptation to turn the
numbers into strings:

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #use-transformed-sink }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #use-transformed-sink }

## Time-Based Processing

Before we start looking at a more involved example we explore the streaming
nature of what Akka Streams can do. Starting from the `factorials` source
we transform the stream by zipping it together with another stream,
represented by a @apidoc[akka.stream.*.Source] that emits the number 0 to 100: the first
number emitted by the `factorials` source is the factorial of zero, the
second is the factorial of one, and so on. We combine these two by forming
strings like `"3! = 6"`.

Scala
:   @@snip [QuickStartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/QuickStartDocSpec.scala) { #add-streams }

Java
:   @@snip [QuickStartDocTest.java](/akka-docs/src/test/java/jdocs/stream/QuickStartDocTest.java) { #add-streams }

All operations so far have been time-independent and could have been performed
in the same fashion on strict collections of elements. The next line
demonstrates that we are in fact dealing with streams that can flow at a
certain speed: we use the @apidoc[throttle](akka.stream.*.Source) {scala="#throttle(elements:Int,per:scala.concurrent.duration.FiniteDuration):FlowOps.this.Repr[Out]" java="#throttle(int,java.time.Duration)"} operator to slow down the stream to 1
element per second.

If you run this program you will see one line printed per second. One aspect
that is not immediately visible deserves mention, though: if you try and set
the streams to produce a billion numbers each then you will notice that your
JVM does not crash with an OutOfMemoryError, even though you will also notice
that running the streams happens in the background, asynchronously (this is the
reason for the auxiliary information to be provided as a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)], in the future). The
secret that makes this work is that Akka Streams implicitly implement pervasive
flow control, all operators respect back-pressure. This allows the throttle
operator to signal to all its upstream sources of data that it can only
accept elements at a certain rate—when the incoming rate is higher than one per
second the throttle operator will assert *back-pressure* upstream.

This is all there is to Akka Streams in a nutshell—glossing over the
fact that there are dozens of sources and sinks and many more stream
transformation operators to choose from, see also @ref:[operator index](operators/index.md).

# Reactive Tweets

A typical use case for stream processing is consuming a live stream of data that we want to extract or aggregate some
other data from. In this example we'll consider consuming a stream of tweets and extracting information concerning Akka from them.

We will also consider the problem inherent to all non-blocking streaming
solutions: *"What if the subscriber is too slow to consume the live stream of
data?"*. Traditionally the solution is often to buffer the elements, but this
can—and usually will—cause eventual buffer overflows and instability of such
systems. Instead Akka Streams depend on internal backpressure signals that
allow to control what should happen in such scenarios.

Here's the data model we'll be working with throughout the quickstart examples:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #model }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #model }

@@@ note

If you would like to get an overview of the used vocabulary first instead of diving head-first
into an actual example you can have a look at the @ref:[Core concepts](stream-flows-and-basics.md#core-concepts) and @ref:[Defining and running streams](stream-flows-and-basics.md#defining-and-running-streams)
sections of the docs, and then come back to this quickstart to see it all pieced together into a simple example application.

@@@

## Transforming and consuming simple streams

The example application we will be looking at is a simple Twitter feed stream from which we'll want to extract certain information,
like for example finding all twitter handles of users who tweet about `#akka`.

In order to prepare our environment by creating an @apidoc[akka.actor.ActorSystem] which will be responsible for running the streams we are about to create:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #system-setup }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #system-setup }

Let's assume we have a stream of tweets readily available. In Akka this is expressed as a @scala[`Source[Out, M]`]@java[`Source<Out, M>`]:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #tweet-source }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #tweet-source }

Streams always start flowing from a @scala[`Source[Out,M1]`]@java[`Source<Out,M1>`] then can continue through @scala[`Flow[In,Out,M2]`]@java[`Flow<In,Out,M2>`] elements or
more advanced operators to finally be consumed by a @scala[`Sink[In,M3]`]@java[`Sink<In,M3>`] @scala[(ignore the type parameters `M1`, `M2`
and `M3` for now, they are not relevant to the types of the elements produced/consumed by these classes – they are
"materialized types", which we'll talk about @ref:[below](#materialized-values-quick))]@java[. The first type parameter—`Tweet` in this case—designates the kind of elements produced
by the source while the `M` type parameters describe the object that is created during
materialization ([see below](#materialized-values-quick))—`NotUsed` (from the `scala.runtime`
package) means that no value is produced, it is the generic equivalent of `void`.]


The operations should look familiar to anyone who has used the Scala Collections library,
however they operate on streams and not collections of data (which is a very important distinction, as some operations
only make sense in streaming and vice versa):

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #authors-filter-map }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #authors-filter-map }

Finally in order to @ref:[materialize](stream-flows-and-basics.md#stream-materialization) and run the stream computation we need to attach
the Flow to a @scala[`Sink`]@java[`Sink<T, M>`] that will get the Flow running. The simplest way to do this is to call
@apidoc[runWith(sink)](akka.stream.*.Source) {scala="#runWith[Mat2](sink:akka.stream.Graph[akka.stream.SinkShape[Out],Mat2])(implicitmaterializer:akka.stream.Materializer):Mat2" java="#runWith(akka.stream.Graph,akka.actor.ClassicActorSystemProvider)"} on a @scala[`Source`]@java[`Source<Out, M>`]. For convenience a number of common Sinks are predefined and collected as @java[static] methods on
the @scala[`Sink` companion object]@java[`Sink class`].
For now let's print each author:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #authors-foreachsink-println }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #authors-foreachsink-println }

or by using the shorthand version (which are defined only for the most popular Sinks such as @apidoc[Sink.fold](akka.stream.*.Sink$) {scala="#fold[U,T](zero:U)(f:(U,T)=%3EU):akka.stream.scaladsl.Sink[T,scala.concurrent.Future[U]]" java="#fold(U,akka.japi.function.Function2)"} and @apidoc[Sink.foreach](akka.stream.*.Sink$) {scala="#foreach[T](f:T=%3EUnit):akka.stream.scaladsl.Sink[T,scala.concurrent.Future[akka.Done]]" java="#foreach(akka.japi.function.Procedure)"}):

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #authors-foreach-println }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #authors-foreach-println }

Materializing and running a stream always requires an `ActorSystem` to be @scala[in implicit scope (or passed in explicitly,
like this: @scaladoc[.runWith(sink)(system)](akka.stream.scaladsl.Source#runWith[Mat2](sink:akka.stream.Graph[akka.stream.SinkShape[Out],Mat2])(implicitmaterializer:akka.stream.Materializer):Mat2))]@java[passed in explicitly, like this: @javadoc[runWith(sink, system)](akka.stream.javadsl.Source#runWith(akka.stream.Graph,akka.stream.Materializer))].

The complete snippet looks like this:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #first-sample }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #first-sample }

## Flattening sequences in streams

In the previous section we were working on 1:1 relationships of elements which is the most common case, but sometimes
we might want to map from one element to a number of elements and receive a "flattened" stream, similarly like `flatMap`
works on Scala Collections. In order to get a flattened stream of hashtags from our stream of tweets we can use the @apidoc[mapConcat](akka.stream.*.Source) {scala="#mapConcat[T](f:Out=%3EIterableOnce[T]):FlowOps.this.Repr[T]" java="#mapConcat(akka.japi.function.Function)"}
operator:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #hashtags-mapConcat }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #hashtags-mapConcat }

@@@ note

The name `flatMap` was consciously avoided due to its proximity with for-comprehensions and monadic composition.
It is problematic for two reasons: @scala[first]@java[firstly], flattening by concatenation is often undesirable in bounded stream processing
due to the risk of deadlock (with merge being the preferred strategy), and @scala[second]@java[secondly], the monad laws would not hold for
our implementation of flatMap (due to the liveness issues).

Please note that the @apidoc[mapConcat](akka.stream.*.Source) {scala="#mapConcat[T](f:Out=%3EIterableOnce[T]):FlowOps.this.Repr[T]" java="#mapConcat(akka.japi.function.Function)"} requires the supplied function to return @scala[an iterable (`f: Out => immutable.Iterable[T]`]@java[a strict collection (`Out f -> java.util.List<T>`)],
whereas `flatMap` would have to operate on streams all the way through.

@@@

## Broadcasting a stream

Now let's say we want to persist all hashtags, as well as all author names from this one live stream.
For example we'd like to write all author handles into one file, and all hashtags into another file on disk.
This means we have to split the source stream into two streams which will handle the writing to these different files.

Elements that can be used to form such "fan-out" (or "fan-in") structures are referred to as "junctions" in Akka Streams.
One of these that we'll be using in this example is called @apidoc[akka.stream.*.Broadcast$], and it emits elements from its
input port to all of its output ports.

Akka Streams intentionally separate the linear stream structures (Flows) from the non-linear, branching ones (Graphs)
in order to offer the most convenient API for both of these cases. Graphs can express arbitrarily complex stream setups
at the expense of not reading as familiarly as collection transformations.

Graphs are constructed using @apidoc[akka.stream.*.GraphDSL$] like this:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #graph-dsl-broadcast }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #graph-dsl-broadcast }

As you can see, @scala[inside the `GraphDSL` we use an implicit graph builder `b` to mutably construct the graph
using the `~>` "edge operator" (also read as "connect" or "via" or "to"). The operator is provided implicitly
by importing `GraphDSL.Implicits._`]@java[we use graph builder `b` to construct the graph using @apidoc[akka.stream.UniformFanOutShape] and @apidoc[akka.stream.*.Flow] s].

@apidoc[GraphDSL.create](akka.stream.*.GraphDSL$) {scala="#create[S&lt;:akka.stream.Shape,IS&lt;:akka.stream.Shape,Mat](graphs:Seq[akka.stream.Graph[IS,Mat]])(buildBlock:akka.stream.scaladsl.GraphDSL.Builder[Seq[Mat]]=%3E(Seq[IS]=%3ES)):akka.stream.Graph[S,Seq[Mat]]" java="#create(java.util.List,akka.japi.function.Function2)"} returns a @apidoc[akka.stream.Graph], in this example a @scala[`Graph[ClosedShape, NotUsed]`]@java[`Graph<ClosedShape,NotUsed>`] where
@apidoc[akka.stream.ClosedShape] means that it is *a fully connected graph* or "closed" - there are no unconnected inputs or outputs.
Since it is closed it is possible to transform the graph into a @apidoc[akka.stream.*.RunnableGraph] using @apidoc[RunnableGraph.fromGraph](akka.stream.*.RunnableGraph$) {scala="#fromGraph[Mat](g:akka.stream.Graph[akka.stream.ClosedShape,Mat]):akka.stream.scaladsl.RunnableGraph[Mat]" java="#fromGraph(akka.stream.Graph)"}.
The `RunnableGraph` can then be @apidoc[run()](akka.stream.*.RunnableGraph) {scala="#run()(implicitmaterializer:akka.stream.Materializer):Mat" java="#run(akka.stream.Materializer)"} to materialize a stream out of it.

Both `Graph` and `RunnableGraph` are *immutable, thread-safe, and freely shareable*.

A graph can also have one of several other shapes, with one or more unconnected ports. Having unconnected ports
expresses a graph that is a *partial graph*. Concepts around composing and nesting graphs in large structures are
explained in detail in @ref:[Modularity, Composition and Hierarchy](stream-composition.md). It is also possible to wrap complex computation graphs
as Flows, Sinks or Sources, which will be explained in detail in
@scala[@ref:[Constructing Sources, Sinks and Flows from Partial Graphs](stream-graphs.md#constructing-sources-sinks-flows-from-partial-graphs)]@java[@ref:[Constructing and combining Partial Graphs](stream-graphs.md#partial-graph-dsl)].

## Back-pressure in action

One of the main advantages of Akka Streams is that they *always* propagate back-pressure information from stream Sinks
(Subscribers) to their Sources (Publishers). It is not an optional feature, and is enabled at all times. To learn more
about the back-pressure protocol used by Akka Streams and all other Reactive Streams compatible implementations read
@ref:[Back-pressure explained](stream-flows-and-basics.md#back-pressure-explained).

A typical problem applications (not using Akka Streams) like this often face is that they are unable to process the incoming data fast enough,
either temporarily or by design, and will start buffering incoming data until there's no more space to buffer, resulting
in either @javadoc[OutOfMemoryError](java.lang.OutOfMemoryError) s or other severe degradations of service responsiveness. With Akka Streams buffering can
and must be handled explicitly. For example, if we are only interested in the "*most recent tweets, with a buffer of 10
elements*" this can be expressed using the @apidoc[buffer](akka.stream.*.Source) {scala="#buffer(size:Int,overflowStrategy:akka.stream.OverflowStrategy):FlowOps.this.Repr[Out]" java="#buffer(int,akka.stream.OverflowStrategy)"} element:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #tweets-slow-consumption-dropHead }

Java
:  @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #tweets-slow-consumption-dropHead }

The `buffer` element takes an explicit and required @apidoc[akka.stream.OverflowStrategy], which defines how the buffer should react
when it receives another element while it is full. Strategies provided include dropping the oldest element (`dropHead`),
dropping the entire buffer, signalling @scala[errors]@java[failures] etc. Be sure to pick and choose the strategy that fits your use case best.

<a id="materialized-values-quick"></a>
## Materialized values

So far we've been only processing data using Flows and consuming it into some kind of external Sink - be it by printing
values or storing them in some external system. However sometimes we may be interested in some value that can be
obtained from the materialized processing pipeline. For example, we want to know how many tweets we have processed.
While this question is not as obvious to give an answer to in case of an infinite stream of tweets (one way to answer
this question in a streaming setting would be to create a stream of counts described as "*up until now*, we've processed N tweets"),
but in general it is possible to deal with finite streams and come up with a nice result such as a total count of elements.

First, let's write such an element counter using @scala[@scaladoc[Sink.fold](akka.stream.scaladsl.Sink$#fold[U,T](zero:U)(f:(U,T)=%3EU):akka.stream.scaladsl.Sink[T,scala.concurrent.Future[U]]) and]@java[@javadoc[Flow.of(Class)](akka.stream.javadsl.Flow#of(java.lang.Class)) and @javadoc[Sink.fold](akka.stream.javadsl.Sink$#fold(U,akka.japi.function.Function2)) to]  see how the types look like:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #tweets-fold-count }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #tweets-fold-count }

@scala[First we prepare a reusable `Flow` that will change each incoming tweet into an integer of value `1`. We'll use this in
order to combine those with a `Sink.fold` that will sum all `Int` elements of the stream and make its result available as
a `Future[Int]`. Next we connect the `tweets` stream to `count` with `via`. Finally we connect the Flow to the previously
prepared Sink using `toMat`]@java[`Sink.fold` will sum all `Integer` elements of the stream and make its result available as
a `CompletionStage<Integer>`. Next we use the `map` method of `tweets` `Source` which will change each incoming tweet
into an integer value `1`.  Finally we connect the Flow to the previously prepared Sink using `toMat`].

Remember those mysterious `Mat` type parameters on @scala[@scaladoc[Source](akka.stream.scaladsl.Source)\[+Out, +Mat\], @scaladoc[Flow](akka.stream.scaladsl.Flow)\[-In, +Out, +Mat\] and @scaladoc[Sink](akka.stream.scaladsl.Sink)\[-In, +Mat\]]@java[@javadoc[Source](akka.stream.javadsl.Source)<Out, Mat>, @javadoc[Flow](akka.stream.javadsl.Flow)<In, Out, Mat> and @javadoc[Sink](akka.stream.javadsl.Sink)<In, Mat>]?
They represent the type of values these processing parts return when materialized. When you chain these together,
you can explicitly combine their materialized values. In our example we used the @scala[@scaladoc[Keep.right](akka.stream.scaladsl.Keep$#right[L,R]:(L,R)=%3ER)]@java[@javadoc[Keep.right()](akka.stream.javadsl.Keep$#right())] predefined function,
which tells the implementation to only care about the materialized type of the operator currently appended to the right.
The materialized type of `sumSink` is @scala[@scaladoc[Future](scala.concurrent.Future)\[Int\]]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)<Integer>] and because of using @scala[`Keep.right`]@java[`Keep.right()`], the resulting @apidoc[akka.stream.*.RunnableGraph]
has also a type parameter of @scala[`Future[Int]`]@java[`CompletionStage<Integer>`].

This step does *not* yet materialize the
processing pipeline, it merely prepares the description of the Flow, which is now connected to a Sink, and therefore can
be `run()`, as indicated by its type: @scala[`RunnableGraph[Future[Int]]`]@java[`RunnableGraph<CompletionStage<Integer>>`]. Next we call @apidoc[run()](akka.stream.*.RunnableGraph) {scala="#run()(implicitmaterializer:akka.stream.Materializer):Mat" java="#run(akka.stream.Materializer)"} which materializes and runs the Flow. The value returned by calling `run()` on a @scala[`RunnableGraph[T]`]@java[`RunnableGraph<T>`] is of type `T`.
In our case this type is @scala[`Future[Int]`]@java[`CompletionStage<Integer>`] which, when completed, will contain the total length of our `tweets` stream.
In case of the stream failing, this future would complete with a Failure.

A @apidoc[akka.stream.*.RunnableGraph] may be reused
and materialized multiple times, because it is only the "blueprint" of the stream. This means that if we materialize a stream,
for example one that consumes a live stream of tweets within a minute, the materialized values for those two materializations
will be different, as illustrated by this example:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #tweets-runnable-flow-materialized-twice }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #tweets-runnable-flow-materialized-twice }

Many elements in Akka Streams provide materialized values which can be used for obtaining either results of computation or
steering these elements which will be discussed in detail in @ref:[Stream Materialization](stream-flows-and-basics.md#stream-materialization). Summing up this section, now we know
what happens behind the scenes when we run this one-liner, which is equivalent to the multi line version above:

Scala
:   @@snip [TwitterStreamQuickstartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/TwitterStreamQuickstartDocSpec.scala) { #tweets-fold-count-oneline }

Java
:   @@snip [TwitterStreamQuickstartDocTest.java](/akka-docs/src/test/java/jdocs/stream/TwitterStreamQuickstartDocTest.java) { #tweets-fold-count-oneline }

@@@ note

`runWith()` is a convenience method that automatically ignores the materialized value of any other operators except
those appended by the `runWith()` itself. In the above example it translates to using @scala[`Keep.right`]@java[`Keep.right()`] as the combiner
for materialized values.

@@@
