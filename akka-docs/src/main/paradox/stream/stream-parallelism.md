# Pipelining and Parallelism

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

Akka Streams operators (be it simple operators on Flows and Sources or graph junctions) are "fused" together
and executed sequentially by default. This avoids the overhead of events crossing asynchronous boundaries but
limits the flow to execute at most one operator at any given time.

In many cases it is useful to be able to concurrently execute the operators of a flow, this is done by explicitly marking
them as asynchronous using the @scala[`async`]@java[`async()`] method. Each operator marked as asynchronous will run in a
dedicated actor internally, while all operators not marked asynchronous will run in one single actor.

We will illustrate through the example of pancake cooking how streams can be used for various processing patterns,
exploiting the available parallelism on modern computers. The setting is the following: both Patrik and Roland
like to make pancakes, but they need to produce sufficient amount in a cooking session to make all of the children
happy. To increase their pancake production throughput they use two frying pans. How they organize their pancake
processing is markedly different.

## Pipelining

Roland uses the two frying pans in an asymmetric fashion. The first pan is only used to fry one side of the
pancake then the half-finished pancake is flipped into the second pan for the finishing fry on the other side.
Once the first frying pan becomes available it gets a new scoop of batter. As an effect, most of the time there
are two pancakes being cooked at the same time, one being cooked on its first side and the second being cooked to
completion.
This is how this setup would look like implemented as a stream:

Scala
:   @@snip [FlowParallelismDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowParallelismDocSpec.scala) { #pipelining }

Java
:   @@snip [FlowParallelismDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowParallelismDocTest.java) { #pipelining }

The two `map` operators in sequence (encapsulated in the "frying pan" flows) will be executed in a pipelined way,
the same way that Roland was using his frying pans:

 1. A `ScoopOfBatter` enters `fryingPan1`
 2. `fryingPan1` emits a HalfCookedPancake once `fryingPan2` becomes available
 3. `fryingPan2` takes the HalfCookedPancake
 4. at this point fryingPan1 already takes the next scoop, without waiting for fryingPan2 to finish

The benefit of pipelining is that it can be applied to any sequence of processing steps that are otherwise not
parallelisable (for example because the result of a processing step depends on all the information from the previous
step). One drawback is that if the processing times of the operators are very different then some of the operators will not
be able to operate at full throughput because they will wait on a previous or subsequent operator most of the time. In the
pancake example frying the second half of the pancake is usually faster than frying the first half, `fryingPan2` will
not be able to operate at full capacity <a id="^1" href="#1">[1]</a>.

@@@ note

Asynchronous stream operators have internal buffers to make communication between them more efficient.
For more details about the behavior of these and how to add additional buffers refer to @ref:[Buffers and working with rate](stream-rate.md).

@@@

## Parallel processing

Patrik uses the two frying pans symmetrically. He uses both pans to fully fry a pancake on both sides, then puts
the results on a shared plate. Whenever a pan becomes empty, he takes the next scoop from the shared bowl of batter.
In essence he parallelizes the same process over multiple pans. This is how this setup will look like if implemented
using streams:

Scala
:   @@snip [FlowParallelismDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowParallelismDocSpec.scala) { #parallelism }

Java
:   @@snip [FlowParallelismDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowParallelismDocTest.java) { #parallelism }

The benefit of parallelizing is that it is easy to scale. In the pancake example
it is easy to add a third frying pan with Patrik's method, but Roland cannot add a third frying pan,
since that would require a third processing step, which is not practically possible in the case of frying pancakes.

One drawback of the example code above is it does not preserve the ordering of pancakes. This might be a problem
if children like to track their "own" pancakes. In those cases the @apidoc[akka.stream.*.Balance$] and @apidoc[akka.stream.*.Merge$] operators should be replaced
by round-robin balancing and merging operators which put in and take out pancakes in a strict order.

A more detailed example of creating a worker pool can be found in the cookbook: @ref:[Balancing jobs to a fixed pool of workers](stream-cookbook.md#cookbook-balance)

## Combining pipelining and parallel processing

The two concurrency patterns that we demonstrated as means to increase throughput are not exclusive.
In fact, it is rather simple to combine the two approaches and streams provide
a nice unifying language to express and compose them.

First, let's look at how we can parallelize pipelined operators. In the case of pancakes this means that we
will employ two chefs, each working using Roland's pipelining method, but we use the two chefs in parallel, just like
Patrik used the two frying pans. This is how it looks like if expressed as streams:

Scala
:   @@snip [FlowParallelismDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowParallelismDocSpec.scala) { #parallel-pipeline }

Java
:   @@snip [FlowParallelismDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowParallelismDocTest.java) { #parallel-pipeline }

The above pattern works well if there are many independent jobs that do not depend on the results of each other, but
the jobs themselves need multiple processing steps where each step builds on the result of
the previous one. In our case individual pancakes do not depend on each other, they can be cooked in parallel, on the
other hand it is not possible to fry both sides of the same pancake at the same time, so the two sides have to be fried
in sequence.

It is also possible to organize parallelized operators into pipelines. This would mean employing four chefs:

 * the first two chefs prepare half-cooked pancakes from batter, in parallel, then putting those on a large enough
flat surface.
 * the second two chefs take these and fry their other side in their own pans, then they put the pancakes on a shared
plate.

This is again straightforward to implement with the streams API:

Scala
:   @@snip [FlowParallelismDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowParallelismDocSpec.scala) { #pipelined-parallel }

Java
:   @@snip [FlowParallelismDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowParallelismDocTest.java) { #pipelined-parallel }

This usage pattern is less common but might be usable if a certain step in the pipeline might take wildly different
times to finish different jobs. The reason is that there are more balance-merge steps in this pattern
compared to the parallel pipelines. This pattern rebalances after each step, while the previous pattern only balances
at the entry point of the pipeline. This only matters however if the processing time distribution has a large
deviation.

> <a id="1" href="#^1">[1]</a> Roland's reason for this seemingly suboptimal procedure is that he prefers the temperature of the second pan
to be slightly lower than the first in order to achieve a more homogeneous result.
