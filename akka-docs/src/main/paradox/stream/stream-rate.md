# Buffers and working with rate

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

When upstream and downstream rates differ, especially when the throughput has spikes, it can be useful to introduce
buffers in a stream. In this chapter we cover how buffers are used in Akka Streams.

<a id="async-stream-buffers"></a>
## Buffers for asynchronous operators

In this section we will discuss internal buffers that are introduced as an optimization when using asynchronous operators.

To run an operator asynchronously it has to be marked explicitly as such using the @scala[@scaladoc[`.async`](akka.stream.Graph#shape:S)]@java[@javadoc[`.async()`](akka.stream.Graph#async--)] method. Being run
asynchronously means that an operator, after handing out an element to its downstream consumer is able to immediately
process the next message. To demonstrate what we mean by this, let's take a look at the following example:

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #pipelining }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #pipelining }

Running the above example, one of the possible outputs looks like this:

```
A: 1
A: 2
B: 1
A: 3
B: 2
C: 1
B: 3
C: 2
C: 3
```

Note that the order is *not* `A:1, B:1, C:1, A:2, B:2, C:2,` which would correspond to the normal fused synchronous
execution model of flows where an element completely passes through the processing pipeline before the next element
enters the flow. The next element is processed by an asynchronous operator as soon as it has emitted the previous one.

While pipelining in general increases throughput, in practice there is a cost of passing an element through the
asynchronous (and therefore thread crossing) boundary which is significant. To amortize this cost Akka Streams uses
a *windowed*, *batching* backpressure strategy internally. It is windowed because as opposed to a [Stop-And-Wait](https://en.wikipedia.org/wiki/Stop-and-wait_ARQ)
protocol multiple elements might be "in-flight" concurrently with requests for elements. It is also batching because
a new element is not immediately requested once an element has been drained from the window-buffer but multiple elements
are requested after multiple elements have been drained. This batching strategy reduces the communication cost of
propagating the backpressure signal through the asynchronous boundary.

While this internal protocol is mostly invisible to the user (apart from its throughput increasing effects) there are
situations when these details get exposed. In all of our previous examples we always assumed that the rate of the
processing chain is strictly coordinated through the backpressure signal causing all operators to process no faster than
the throughput of the connected chain. There are tools in Akka Streams however that enable the rates of different segments
of a processing chain to be "detached" or to define the maximum throughput of the stream through external timing sources.
These situations are exactly those where the internal batching buffering strategy suddenly becomes non-transparent.

### Internal buffers and their effect

As we have explained, for performance reasons Akka Streams introduces a buffer for every asynchronous operator.
The purpose of these buffers is solely optimization, in fact the size of 1 would be the most natural choice if there
would be no need for throughput improvements. Therefore it is recommended to keep these buffer sizes small,
and increase them only to a level suitable for the throughput requirements of the application. Default buffer sizes
can be set through configuration:

```
akka.stream.materializer.max-input-buffer-size = 16
```

Alternatively they can be set per stream by adding an attribute to the complete `RunnableGraph` or on smaller segments
of the stream it is possible by defining a separate
@scala[@scaladoc[`Flow`](akka.stream.scaladsl.Flow)]@java[@javadoc[`Flow`](akka.stream.javadsl.Flow)] with these attributes:

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #section-buffer }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #section-buffer }

Here is an example of a code that demonstrate some of the issues caused by internal buffers:

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #buffering-abstraction-leak }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #buffering-abstraction-leak }

Running the above example one would expect the number *3* to be printed in every 3 seconds (the `conflateWithSeed`
step here is configured so that it counts the number of elements received before the downstream @scala[@scaladoc[`ZipWith`](akka.stream.scaladsl.ZipWith$)]@java[@javadoc[`ZipWith`](akka.stream.javadsl.ZipWith$)] consumes
them). What is being printed is different though, we will see the number *1*. The reason for this is the internal
buffer which is by default 16 elements large, and prefetches elements before the @scala[@scaladoc[`ZipWith`](akka.stream.scaladsl.ZipWith$)]@java[@javadoc[`ZipWith`](akka.stream.javadsl.ZipWith$)] starts consuming them.
It is possible to fix this issue by changing the buffer size of @scala[@scaladoc[`ZipWith`](akka.stream.scaladsl.ZipWith$)]@java[@javadoc[`ZipWith`](akka.stream.javadsl.ZipWith$)] (or the whole graph) to 1. We will still see
a leading 1 though which is caused by an initial prefetch of the @scala[@scaladoc[`ZipWith`](akka.stream.scaladsl.ZipWith$)]@java[@javadoc[`ZipWith`](akka.stream.javadsl.ZipWith$)] element.

@@@ note

In general, when time or rate driven operators exhibit strange behavior, one of the first solutions to try
should be to decrease the input buffer of the affected elements to 1.

@@@

## Buffers in Akka Streams

In this section we will discuss *explicit* user defined buffers that are part of the domain logic of the stream processing
pipeline of an application.

The example below will ensure that 1000 jobs (but not more) are dequeued from an external (imaginary) system and
stored locally in memory - relieving the external system:

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #explicit-buffers-backpressure }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #explicit-buffers-backpressure }


The next example will also queue up 1000 jobs locally, but if there are more jobs waiting
in the imaginary external systems, it makes space for the new element by
dropping one element from the *tail* of the buffer. Dropping from the tail is a very common strategy but
it must be noted that this will drop the *youngest* waiting job. If some "fairness" is desired in the sense that
we want to be nice to jobs that has been waiting for long, then this option can be useful.

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #explicit-buffers-droptail }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #explicit-buffers-droptail }

Instead of dropping the youngest element from the tail of the buffer a new element can be dropped without
enqueueing it to the buffer at all.

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #explicit-buffers-dropnew }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #explicit-buffers-dropnew }

Here is another example with a queue of 1000 jobs, but it makes space for the new element by
dropping one element from the *head* of the buffer. This is the *oldest*
waiting job. This is the preferred strategy if jobs are expected to be
resent if not processed in a certain period. The oldest element will be
retransmitted soon, (in fact a retransmitted duplicate might be already in the queue!)
so it makes sense to drop it first.

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #explicit-buffers-drophead }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #explicit-buffers-drophead }

Compared to the dropping strategies above, dropBuffer drops all the 1000
jobs it has enqueued once the buffer gets full. This aggressive strategy
is useful when dropping jobs is preferred to delaying jobs.

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #explicit-buffers-dropbuffer }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #explicit-buffers-dropbuffer }

If our imaginary external job provider is a client using our API, we might
want to enforce that the client cannot have more than 1000 queued jobs
otherwise we consider it flooding and terminate the connection. This is
achievable by the error strategy which fails the stream
once the buffer gets full.

Scala
:   @@snip [StreamBuffersRateSpec.scala](/akka-docs/src/test/scala/docs/stream/StreamBuffersRateSpec.scala) { #explicit-buffers-fail }

Java
:   @@snip [StreamBuffersRateDocTest.java](/akka-docs/src/test/java/jdocs/stream/StreamBuffersRateDocTest.java) { #explicit-buffers-fail }

## Rate transformation

### Understanding conflate

When a fast producer can not be informed to slow down by backpressure or some other signal, `conflate` might be
useful to combine elements from a producer until a demand signal comes from a consumer.

Below is an example snippet that summarizes fast stream of elements to a standard deviation, mean and count of elements
that have arrived while the stats have been calculated.

Scala
:   @@snip [RateTransformationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RateTransformationDocSpec.scala) { #conflate-summarize }

Java
:   @@snip [RateTransformationDocTest.java](/akka-docs/src/test/java/jdocs/stream/RateTransformationDocTest.java) { #conflate-summarize }

This example demonstrates that such flow's rate is decoupled. The element rate at the start of the flow can be much
higher than the element rate at the end of the flow.

Another possible use of `conflate` is to not consider all elements for summary when the producer starts getting too fast.
The example below demonstrates how `conflate` can be used to randomly drop elements when the consumer is not able
to keep up with the producer.

Scala
:   @@snip [RateTransformationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RateTransformationDocSpec.scala) { #conflate-sample }

Java
:   @@snip [RateTransformationDocTest.java](/akka-docs/src/test/java/jdocs/stream/RateTransformationDocTest.java) { #conflate-sample }

See also @ref:[`conflate`](operators/Source-or-Flow/conflate.md) and @ref:[conflateWithSeed`](operators/Source-or-Flow/conflateWithSeed.md) for more information and examples.


### Understanding extrapolate and expand

Now we will discuss two operators, `extrapolate` and `expand`, helping to deal with slow producers that are unable to keep 
up with the demand coming from consumers.
They allow for additional values to be sent as elements to a consumer.

As a simple use case of `extrapolate`, here is a flow that repeats the last emitted element to a consumer, whenever 
the consumer signals demand and the producer cannot supply new elements yet.

Scala
:   @@snip [RateTransformationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RateTransformationDocSpec.scala) { #extrapolate-last }

Java
:   @@snip [RateTransformationDocTest.java](/akka-docs/src/test/java/jdocs/stream/RateTransformationDocTest.java) { #extrapolate-last }

For situations where there may be downstream demand before any element is emitted from upstream, 
you can use the `initial` parameter of `extrapolate` to "seed" the stream.

Scala
:   @@snip [RateTransformationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RateTransformationDocSpec.scala) { #extrapolate-seed }

Java
:   @@snip [RateTransformationDocTest.java](/akka-docs/src/test/java/jdocs/stream/RateTransformationDocTest.java) { #extrapolate-seed }

`extrapolate` and `expand` also allow to produce meta-information based on demand signalled from the downstream. 
Leveraging this, here is a flow that tracks and reports a drift between a fast consumer and a slow producer. 

Scala
:   @@snip [RateTransformationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RateTransformationDocSpec.scala) { #extrapolate-drift }

Java
:   @@snip [RateTransformationDocTest.java](/akka-docs/src/test/java/jdocs/stream/RateTransformationDocTest.java) { #extrapolate-drift }

And here's a more concise representation with `expand`.

Scala
:   @@snip [RateTransformationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RateTransformationDocSpec.scala) { #expand-drift }

Java
:   @@snip [RateTransformationDocTest.java](/akka-docs/src/test/java/jdocs/stream/RateTransformationDocTest.java) { #expand-drift }

The difference is due to the different handling of the `Iterator`-generating argument.

While `extrapolate` uses an `Iterator` only when there is unmet downstream demand, `expand` _always_ creates 
an `Iterator` and emits elements downstream from it.

This makes `expand` able to transform or even filter out (by providing an empty `Iterator`) the "original" elements.
 
Regardless, since we provide a non-empty `Iterator` in both examples, this means that the
output of this flow is going to report a drift of zero if the producer is fast enough - or a larger drift otherwise.

See also @ref:[`extrapolate`](operators/Source-or-Flow/extrapolate.md) and @ref:[`expand`](operators/Source-or-Flow/expand.md) for more information and examples.
