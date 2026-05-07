# Working with Graphs

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

In Akka Streams computation graphs are not expressed using a fluent DSL like linear computations are, instead they are
written in a more graph-resembling DSL which aims to make translating graph drawings (e.g. from notes taken
from design discussions, or illustrations in protocol specifications) to and from code simpler. In this section we'll
dive into the multiple ways of constructing and re-using graphs, as well as explain common pitfalls and how to avoid them.

Graphs are needed whenever you want to perform any kind of fan-in ("multiple inputs") or fan-out ("multiple outputs") operations.
Considering linear Flows to be like roads, we can picture graph operations as junctions: multiple flows being connected at a single point.
Some operators which are common enough and fit the linear style of Flows, such as `concat` (which concatenates two
streams, such that the second one is consumed after the first one has completed), may have shorthand methods defined on
`Flow` or `Source` themselves, however you should keep in mind that those are also implemented as graph junctions.

<a id="graph-dsl"></a>
## Constructing Graphs

Graphs are built from simple Flows which serve as the linear connections within the graphs as well as junctions
which serve as fan-in and fan-out points for Flows. Thanks to the junctions having meaningful types based on their behavior
and making them explicit elements these elements should be rather straightforward to use.

Akka Streams currently provide these junctions (for a detailed list see the @ref[operator index](operators/index.md)):

 * **Fan-out**

    * @scala[`Broadcast[T]`]@java[`Broadcast<T>`] – *(1 input, N outputs)* given an input element emits to each output
    * @scala[`Balance[T]`]@java[`Balance<T>`] – *(1 input, N outputs)* given an input element emits to one of its output ports
    * @scala[`Partition[T]]`]@java[`Partition<T>`] – *(1 input, N outputs)* given an input element emits to specified output based on a partition function
    * @scala[`UnzipWith[In,A,B,...]`]@java[`UnzipWith<In,A,B,...>`] – *(1 input, N outputs)* takes a function of 1 input that given a value for each input emits N output elements (where N <= 20)
    * @scala[`UnZip[A,B]`]@java[`UnZip<A,B>`] – *(1 input, 2 outputs)* splits a stream of @scala[`(A,B)`]@java[`Pair<A,B>`] tuples into two streams, one of type `A` and one of type `B`

 * **Fan-in**

    * @scala[`Merge[In]`]@java[`Merge<In>`] – *(N inputs , 1 output)* picks randomly from inputs pushing them one by one to its output
    * @scala[`MergePreferred[In]`]@java[`MergePreferred<In>`] – like `Merge` but if elements are available on `preferred` port, it picks from it, otherwise randomly from `others`
    * @scala[`MergePrioritized[In]`]@java[`MergePrioritized<In>`] – like `Merge` but if elements are available on all input ports, it picks from them randomly based on their `priority`
    * @scala[`MergeLatest[In]`]@java[`MergeLatest<In>`] – *(N inputs, 1 output)* emits `List[In]`, when i-th input stream emits element, then i-th element in emitted list is updated
    * @scala[`MergeSequence[In]`]@java[`MergeSequence<In>`] – *(N inputs, 1 output)* emits `List[In]`, where the input streams must represent a partitioned sequence that must be merged back together in order
    * @scala[`ZipWith[A,B,...,Out]`]@java[`ZipWith<A,B,...,Out>`] – *(N inputs, 1 output)* which takes a function of N inputs that given a value for each input emits 1 output element
    * @scala[`Zip[A,B]`]@java[`Zip<A,B>`] – *(2 inputs, 1 output)* is a `ZipWith` specialised to zipping input streams of `A` and `B` into a @scala[`(A,B)`]@java[`Pair(A,B)`] tuple stream
    * @scala[`Concat[A]`]@java[`Concat<A>`] – *(2 inputs, 1 output)* concatenates two streams (first consume one, then the second one)

One of the goals of the GraphDSL DSL is to look similar to how one would draw a graph on a whiteboard, so that it is
simple to translate a design from whiteboard to code and be able to relate those two. Let's illustrate this by translating
the below hand drawn graph into Akka Streams:

![simple-graph-example.png](../images/simple-graph-example.png)

Such graph is simple to translate to the Graph DSL since each linear element corresponds to a `Flow`,
and each circle corresponds to either a `Junction` or a `Source` or `Sink` if it is beginning
or ending a `Flow`. @scala[Junctions must always be created with defined type parameters, as otherwise the `Nothing` type
will be inferred.]

Scala
:   @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #simple-graph-dsl }

Java
:   @@snip [GraphDSLTest.java](/akka-stream-tests/src/test/java/akka/stream/javadsl/GraphDslTest.java) { #simple-graph-dsl }

@@@ note

Junction *reference equality* defines *graph node equality* (i.e. the same merge *instance* used in a GraphDSL
refers to the same location in the resulting graph).

@@@

@scala[Notice the `import GraphDSL.Implicits._` which brings into scope the `~>` operator (read as "edge", "via" or "to")
and its inverted counterpart `<~` (for noting down flows in the opposite direction where appropriate).]

By looking at the snippets above, it should be apparent that the @scala[`GraphDSL.Builder`]@java[`builder`] object is *mutable*.
@scala[It is used (implicitly) by the `~>` operator, also making it a mutable operation as well.]
The reason for this design choice is to enable simpler creation of complex graphs, which may even contain cycles.
Once the GraphDSL has been constructed though, the @scala[`GraphDSL`]@java[`RunnableGraph`] instance *is immutable, thread-safe, and freely shareable*.
The same is true of all operators—sources, sinks, and flows—once they are constructed.
This means that you can safely re-use one given Flow or junction in multiple places in a processing graph.

We have seen examples of such re-use already above: the merge and broadcast junctions were imported
into the graph using `builder.add(...)`, an operation that will make a copy of the blueprint that
is passed to it and return the inlets and outlets of the resulting copy so that they can be wired up.
Another alternative is to pass existing graphs—of any shape—into the factory method that produces a
new graph. The difference between these approaches is that importing using `builder.add(...)` ignores the
materialized value of the imported graph while importing via the factory method allows its inclusion;
for more details see @ref[Stream Materialization](stream-flows-and-basics.md#stream-materialization).

In the example below we prepare a graph that consists of two parallel streams,
in which we re-use the same instance of `Flow`, yet it will properly be
materialized as two connections between the corresponding Sources and Sinks:

Scala
:   @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #graph-dsl-reusing-a-flow }

Java
:   @@snip [GraphDSLTest.java](/akka-stream-tests/src/test/java/akka/stream/javadsl/GraphDslTest.java) { #graph-dsl-reusing-a-flow }

In some cases we may have a list of graph elements, for example if they are dynamically created. 
If these graphs have similar signatures, we can construct a graph collecting all their materialized values as a collection:

Scala
:   @@snip [GraphOpsIntegrationSpec.scala](/akka-stream-tests/src/test/scala/akka/stream/scaladsl/GraphOpsIntegrationSpec.scala) { #graph-from-list }

Java
:   @@snip [GraphDSLTest.java](/akka-stream-tests/src/test/java/akka/stream/javadsl/GraphDslTest.java) { #graph-from-list }


<a id="partial-graph-dsl"></a>
## Constructing and combining Partial Graphs

Sometimes it is not possible (or needed) to construct the entire computation graph in one place, but instead construct
all of its different phases in different places and in the end connect them all into a complete graph and run it.

This can be achieved by @scala[returning a different `Shape` than `ClosedShape`, for example `FlowShape(in, out)`, from the
function given to `GraphDSL.create`. See @ref:[Predefined shapes](#predefined-shapes) for a list of such predefined shapes.
Making a `Graph` a `RunnableGraph`]@java[using the returned `Graph` from `GraphDSL.create()` rather than
passing it to `RunnableGraph.fromGraph()` to wrap it in a `RunnableGraph`.The reason of representing it as a different type is that a
`RunnableGraph`] requires all ports to be connected, and if they are not
it will throw an exception at construction time, which helps to avoid simple
wiring errors while working with graphs. A partial graph however allows
you to return the set of yet to be connected ports from the code block that
performs the internal wiring.

Let's imagine we want to provide users with a specialized element that given 3 inputs will pick
the greatest int value of each zipped triple. We'll want to expose 3 input ports (unconnected sources) and one output port
(unconnected sink).

Scala
:   @@snip [StreamPartialGraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamPartialGraphDSLDocSpec.scala) { #simple-partial-graph-dsl }  

Java
:   @@snip [StreamPartialGraphDSLDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamPartialGraphDSLDocTest.java) { #simple-partial-graph-dsl }

@@@ note

While the above example shows composing two 2-input `ZipWith`s, in reality ZipWith already provides 
numerous overloads including a 3 (and many more) parameter versions. So this could be implemented 
using one ZipWith using the 3 parameter version, like this: @scala[`ZipWith((a, b, c) => out)`]@java[`ZipWith.create((a, b, c) -> out)`].
(The ZipWith with N input has N+1 type parameter; the last type param is the output type.)

@@@

As you can see, first we construct the partial graph that @scala[contains all the zipping and comparing of stream
elements. This partial graph will have three inputs and one output, wherefore we use the `UniformFanInShape`]@java[describes how to compute the maximum of two input streams.
then we reuse that twice while constructing the partial graph that extends this to three input streams].
Then we import it (all of its nodes and connections) explicitly into the @scala[closed graph built in the second step]@java[last graph] in which all
the undefined elements are rewired to real sources and sinks. The graph can then be run and yields the expected result.

@@@ warning

Please note that `GraphDSL` is not able to provide compile time type-safety about whether or not all
elements have been properly connected—this validation is performed as a runtime check during the graph's instantiation.

A partial graph also verifies that all ports are either connected or part of the returned `Shape`.

@@@

<a id="constructing-sources-sinks-flows-from-partial-graphs"></a>
## Constructing Sources, Sinks and Flows from Partial Graphs

Instead of treating a @scala[partial graph]@java[`Graph`] as a collection of flows and junctions which may not yet all be
connected it is sometimes useful to expose such a complex graph as a simpler structure,
such as a `Source`, `Sink` or `Flow`.

In fact, these concepts can be expressed as special cases of a partially connected graph:

 * `Source` is a partial graph with *exactly one* output, that is it returns a `SourceShape`.
 * `Sink` is a partial graph with *exactly one* input, that is it returns a `SinkShape`.
 * `Flow` is a partial graph with *exactly one* input and *exactly one* output, that is it returns a `FlowShape`.

Being able to hide complex graphs inside of simple elements such as Sink / Source / Flow enables you to create one
complex element and from there on treat it as simple compound operator for linear computations.

In order to create a Source from a graph the method `Source.fromGraph` is used, to use it we must have a
@scala[`Graph[SourceShape, T]`]@java[`Graph` with a `SourceShape`]. This is constructed using
@scala[`GraphDSL.create` and returning a `SourceShape` from the function passed in]@java[`GraphDSL.create` and providing building a `SourceShape` graph].
The single outlet must be provided to the `SourceShape.of` method and will become
“the sink that must be attached before this Source can run”.

Refer to the example below, in which we create a Source that zips together two numbers, to see this graph
construction in action:

Scala
:   @@snip [StreamPartialGraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamPartialGraphDSLDocSpec.scala) { #source-from-partial-graph-dsl }    

Java
:   @@snip [StreamPartialGraphDSLDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamPartialGraphDSLDocTest.java) { #source-from-partial-graph-dsl }


Similarly the same can be done for a @scala[`Sink[T]`]@java[`Sink<T>`], using `SinkShape.of` in which case the provided value
must be an @scala[`Inlet[T]`]@java[`Inlet<T>`]. For defining a @scala[`Flow[T]`]@java[`Flow<T>`] we need to expose both an @scala[inlet and an outlet]@java[undefined source and sink]:

Scala
:   @@snip [StreamPartialGraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamPartialGraphDSLDocSpec.scala) { #flow-from-partial-graph-dsl }    

Java
:   @@snip [StreamPartialGraphDSLDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamPartialGraphDSLDocTest.java) { #flow-from-partial-graph-dsl }


## Combining Sources and Sinks with simplified API

There is a simplified API you can use to combine sources and sinks with junctions like:
@scala[`Broadcast[T]`, `Balance[T]`, `Merge[In]` and `Concat[A]`]@java[`Broadcast<T>`, `Balance<T>`, `Merge<In>` and `Concat<A>`]
without the need for using the Graph DSL. The combine method takes care of constructing
the necessary graph underneath. In following example we combine two sources into one (fan-in):

Scala
:   @@snip [StreamPartialGraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamPartialGraphDSLDocSpec.scala) { #source-combine }   

Java
:   @@snip [StreamPartialGraphDSLDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamPartialGraphDSLDocTest.java) { #source-combine }


The same can be done for a @scala[`Sink[T]`]@java[`Sink`] but in this case it will be fan-out:

Scala
:   @@snip [StreamPartialGraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamPartialGraphDSLDocSpec.scala) { #sink-combine }  

Java
:   @@snip [StreamPartialGraphDSLDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamPartialGraphDSLDocTest.java) { #sink-combine }


## Building reusable Graph components

It is possible to build reusable, encapsulated components of arbitrary input and output ports using the graph DSL.

As an example, we will build a graph junction that represents a pool of workers, where a worker is expressed
as a @scala[`Flow[I,O,_]`]@java[`Flow<I,O,M>`], i.e. a simple transformation of jobs of type `I` to results of type `O` (as you have seen
already, this flow can actually contain a complex graph inside). Our reusable worker pool junction will
not preserve the order of the incoming jobs (they are assumed to have a proper ID field) and it will use a `Balance`
junction to schedule jobs to available workers. On top of this, our junction will feature a "fastlane", a dedicated port
where jobs of higher priority can be sent.

Altogether, our junction will have two input ports of type `I` (for the normal and priority jobs) and an output port
of type `O`. To represent this interface, we need to define a custom `Shape`. The following lines show how to do that.

Scala
:   @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #graph-dsl-components-shape }  



## Predefined shapes

In general a custom `Shape` needs to be able to provide all its input and output ports, be able to copy itself, and also be
able to create a new instance from given ports. There are some predefined shapes provided to avoid unnecessary
boilerplate:

 * `SourceShape`, `SinkShape`, `FlowShape` for simpler shapes,
 * `UniformFanInShape` and `UniformFanOutShape` for junctions with multiple input (or output) ports
of the same type,
 * `FanInShape1`, `FanInShape2`, ..., `FanOutShape1`, `FanOutShape2`, ... for junctions
with multiple input (or output) ports of different types.

Since our shape has two input ports and one output port, we can use the `FanInShape` DSL to define
our custom shape:

Scala
:  @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #graph-dsl-components-shape2 }  



Now that we have a `Shape` we can wire up a Graph that represents
our worker pool. First, we will merge incoming normal and priority jobs using `MergePreferred`, then we will send the jobs
to a `Balance` junction which will fan-out to a configurable number of workers (flows), finally we merge all these
results together and send them out through our only output port. This is expressed by the following code:

Scala
:   @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #graph-dsl-components-create }    




All we need to do now is to use our custom junction in a graph. The following code simulates some simple workers
and jobs using plain strings and prints out the results. Actually we used *two* instances of our worker pool junction
using `add()` twice.

Scala
:   @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #graph-dsl-components-use }




<a id="bidi-flow"></a>
## Bidirectional Flows

A graph topology that is often useful is that of two flows going in opposite
directions. Take for example a codec operator that serializes outgoing messages
and deserializes incoming octet streams. Another such operator could add a framing
protocol that attaches a length header to outgoing data and parses incoming
frames back into the original octet stream chunks. These two operators are meant
to be composed, applying one atop the other as part of a protocol stack. For
this purpose exists the special type `BidiFlow` which is a graph that
has exactly two open inlets and two open outlets. The corresponding shape is
called `BidiShape` and is defined like this:

@@snip [Shape.scala](/akka-stream/src/main/scala/akka/stream/Shape.scala) { #bidi-shape }   


A bidirectional flow is defined just like a unidirectional `Flow` as
demonstrated for the codec mentioned above:

Scala
:   @@snip [BidiFlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/BidiFlowDocSpec.scala) { #codec }

Java
:   @@snip [BidiFlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/BidiFlowDocTest.java) { #codec }


The first version resembles the partial graph constructor, while for the simple
case of a functional 1:1 transformation there is a concise convenience method
as shown on the last line. The implementation of the two functions is not
difficult either:

Scala
:   @@snip [BidiFlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/BidiFlowDocSpec.scala) { #codec-impl }  

Java
:   @@snip [BidiFlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/BidiFlowDocTest.java) { #codec-impl }


In this way you can integrate any other serialization library that
turns an object into a sequence of bytes.

The other operator that we talked about is a little more involved since reversing
a framing protocol means that any received chunk of bytes may correspond to
zero or more messages. This is best implemented using @ref[`GraphStage`](stream-customize.md)
(see also @ref[Custom processing with GraphStage](stream-customize.md#graphstage)).

Scala
:   @@snip [BidiFlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/BidiFlowDocSpec.scala) { #framing }  

Java
:   @@snip [BidiFlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/BidiFlowDocTest.java) { #framing }


With these implementations we can build a protocol stack and test it:

Scala
:   @@snip [BidiFlowDocSpec.scala](/akka-docs/src/test/scala/docs/stream/BidiFlowDocSpec.scala) { #compose }  

Java
:   @@snip [BidiFlowDocTest.java](/akka-docs/src/test/java/jdocs/stream/BidiFlowDocTest.java) { #compose }


This example demonstrates how `BidiFlow` subgraphs can be hooked
together and also turned around with the @scala[`.reversed`]@java[`.reversed()`] method. The test
simulates both parties of a network communication protocol without actually
having to open a network connection—the flows can be connected directly.

<a id="graph-matvalue"></a>
## Accessing the materialized value inside the Graph

In certain cases it might be necessary to feed back the materialized value of a Graph (partial, closed or backing a
Source, Sink, Flow or BidiFlow). This is possible by using `builder.materializedValue` which gives an `Outlet` that
can be used in the graph as an ordinary source or outlet, and which will eventually emit the materialized value.
If the materialized value is needed at more than one place, it is possible to call `materializedValue` any number of
times to acquire the necessary number of outlets.

Scala
:   @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #graph-dsl-matvalue }

Java
:   @@snip [GraphDSLTest.java](/akka-stream-tests/src/test/java/akka/stream/javadsl/GraphDslTest.java) { #graph-dsl-matvalue }


Be careful not to introduce a cycle where the materialized value actually contributes to the materialized value.
The following example demonstrates a case where the materialized @scala[`Future`]@java[`CompletionStage`] of a fold is fed back to the fold itself.

Scala
:  @@snip [GraphDSLDocSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphDSLDocSpec.scala) { #graph-dsl-matvalue-cycle }

Java
:  @@snip [GraphDSLTest.java](/akka-stream-tests/src/test/java/akka/stream/javadsl/GraphDslTest.java) { #graph-dsl-matvalue-cycle }


<a id="graph-cycles"></a>
## Graph cycles, liveness and deadlocks

Cycles in bounded stream topologies need special considerations to avoid potential deadlocks and other liveness issues.
This section shows several examples of problems that can arise from the presence of feedback arcs in stream processing
graphs.

In the following examples runnable graphs are created but do not run because each have some issue and will deadlock after start.
`Source` variable is not defined as the nature and number of element does not matter for described problems.

The first example demonstrates a graph that contains a naïve cycle.
The graph takes elements from the source, prints them, then broadcasts those elements
to a consumer (we just used `Sink.ignore` for now) and to a feedback arc that is merged back into the main stream via
a `Merge` junction.

@@@ note

The graph DSL allows the connection arrows to be reversed, which is particularly handy when writing cycles—as we will
see there are cases where this is very helpful.

@@@

Scala
:   @@snip [GraphCyclesSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphCyclesSpec.scala) { #deadlocked }  

Java
:   @@snip [GraphCyclesDocTest.java](/akka-docs/src/test/java/jdocs/stream/GraphCyclesDocTest.java) { #deadlocked }


Running this we observe that after a few numbers have been printed, no more elements are logged to the console -
all processing stops after some time. After some investigation we observe that:

 * through merging from `source` we increase the number of elements flowing in the cycle
 * by broadcasting back to the cycle we do not decrease the number of elements in the cycle

Since Akka Streams (and Reactive Streams in general) guarantee bounded processing (see the "Buffering" section for more
details) it means that only a bounded number of elements are buffered over any time span. Since our cycle gains more and
more elements, eventually all of its internal buffers become full, backpressuring `source` forever. To be able
to process more elements from `source` elements would need to leave the cycle somehow.

If we modify our feedback loop by replacing the `Merge` junction with a `MergePreferred` we can avoid the deadlock.
`MergePreferred` is unfair as it always tries to consume from a preferred input port if there are elements available
before trying the other lower priority input ports. Since we feed back through the preferred port it is always guaranteed
that the elements in the cycles can flow.

Scala
:   @@snip [GraphCyclesSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphCyclesSpec.scala) { #unfair }

Java
:   @@snip [GraphCyclesDocTest.java](/akka-docs/src/test/java/jdocs/stream/GraphCyclesDocTest.java) { #unfair }


If we run the example we see that the same sequence of numbers are printed
over and over again, but the processing does not stop. Hence, we avoided the deadlock, but `source` is still
back-pressured forever, because buffer space is never recovered: the only action we see is the circulation of a couple
of initial elements from `source`.

@@@ note

What we see here is that in certain cases we need to choose between boundedness and liveness. Our first example would
not deadlock if there were an infinite buffer in the loop, or vice versa, if the elements in the cycle were 
balanced (as many elements are removed as many are injected) then there would be no deadlock.

@@@

To make our cycle both live (not deadlocking) and fair we can introduce a dropping element on the feedback arc. In this
case we chose the `buffer()` operation giving it a dropping strategy `OverflowStrategy.dropHead`.

Scala
:   @@snip [GraphCyclesSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphCyclesSpec.scala) { #dropping }

Java
:   @@snip [GraphCyclesDocTest.java](/akka-docs/src/test/java/jdocs/stream/GraphCyclesDocTest.java) { #dropping }


If we run this example we see that

 * The flow of elements does not stop, there are always elements printed
 * We see that some of the numbers are printed several times over time (due to the feedback loop) but on average
the numbers are increasing in the long term

This example highlights that one solution to avoid deadlocks in the presence of potentially unbalanced cycles
(cycles where the number of circulating elements are unbounded) is to drop elements. An alternative would be to
define a larger buffer with `OverflowStrategy.fail` which would fail the stream instead of deadlocking it after
all buffer space has been consumed.

As we discovered in the previous examples, the core problem was the unbalanced nature of the feedback loop. We
circumvented this issue by adding a dropping element, but now we want to build a cycle that is balanced from
the beginning instead. To achieve this we modify our first graph by replacing the `Merge` junction with a `ZipWith`.
Since `ZipWith` takes one element from `source` *and* from the feedback arc to inject one element into the cycle,
we maintain the balance of elements.

Scala
:   @@snip [GraphCyclesSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphCyclesSpec.scala) { #zipping-dead }    

Java
:   @@snip [GraphCyclesDocTest.java](/akka-docs/src/test/java/jdocs/stream/GraphCyclesDocTest.java) { #zipping-dead }


Still, when we try to run the example it turns out that no element is printed at all! After some investigation we
realize that:

 * In order to get the first element from `source` into the cycle we need an already existing element in the cycle
 * In order to get an initial element in the cycle we need an element from `source`

These two conditions are a typical "chicken-and-egg" problem. The solution is to inject an initial
element into the cycle that is independent from `source`. We do this by using a `Concat` junction on the backwards
arc that injects a single element using `Source.single`.

Scala
:   @@snip [GraphCyclesSpec.scala](/akka-docs/src/test/scala/docs/stream/GraphCyclesSpec.scala) { #zipping-live }    

Java
:   @@snip [GraphCyclesDocTest.java](/akka-docs/src/test/java/jdocs/stream/GraphCyclesDocTest.java) { #zipping-live }


When we run the above example we see that processing starts and never stops. The important takeaway from this example
is that balanced cycles often need an initial "kick-off" element to be injected into the cycle.
