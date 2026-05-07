# Streams Cookbook

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

This is a collection of patterns to demonstrate various usage of the Akka Streams API by solving small targeted
problems in the format of "recipes". The purpose of this page is to give inspiration and ideas how to approach
various small tasks involving streams. The recipes in this page can be used directly as-is, but they are most powerful as
starting points: customization of the code snippets is warmly encouraged. The recipes can be extended or can provide a 
basis for the implementation of other [patterns](https://doc.akka.io/libraries/alpakka/current/patterns.html) involving
[Alpakka](https://doc.akka.io/libraries/alpakka/current).

This part also serves as supplementary material for the main body of documentation. It is a good idea to have this page
open while reading the manual and look for examples demonstrating various streaming concepts
as they appear in the main body of documentation.

If you need a quick reference of the available operators used in the recipes see @ref:[operator index](operators/index.md).

## Working with Flows

In this collection we show simple recipes that involve linear flows. The recipes in this section are rather
general, more targeted recipes are available as separate sections (@ref:[Buffers and working with rate](stream-rate.md), @ref:[Working with streaming IO](stream-io.md)).

### Logging in streams

**Situation:** During development it is sometimes helpful to see what happens in a particular section of a stream.

The simplest solution is to use a `map` operation and use `println` to print the elements received to the console.
While this recipe is rather simplistic, it is often suitable for a quick debug session.

Scala
:   @@snip [RecipeLoggingElements.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeLoggingElements.scala) { #println-debug }

Java
:   @@snip [RecipeLoggingElements.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeLoggingElements.java) { #println-debug }

Another approach to logging is to use `log()` operation. This approach gives you more fine-grained control of logging levels for
elements flowing through the stream, finish and failure of the stream.

Scala
:   @@snip [RecipeLoggingElements.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeLoggingElements.scala) { #log-custom }

Java
:   @@snip [RecipeLoggingElements.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeLoggingElements.java) { #log-custom }

### Creating a source that continuously evaluates a function

**Situation:** A source is required that continuously provides elements obtained by evaluating a given function, so long as there is demand.

The simplest implementation is to use a `Source.repeat` that produces some arbitrary element - e.g. `NotUsed` -
and then map those elements to the function evaluation. E.g. if we have some `builderFunction()`, we can use:

Scala
:   @@snip [RecipeSourceFromFunction.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeSourceFromFunction.scala) { #source-from-function }

Java
:   @@snip [RecipeSourceFromFunction.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeSourceFromFunction.java) { #source-from-function }

Note: if the element-builder function touches mutable state, then a guaranteed single-threaded source should be used
instead; e.g. `Source.unfold` or `Source.unfoldResource`.

### Flattening a stream of sequences

**Situation:** A stream is given as a stream of sequence of elements, but a stream of elements needed instead, streaming
all the nested elements inside the sequences separately.

The `mapConcat` operation can be used to implement a one-to-many transformation of elements using a mapper function
in the form of @scala[`In => immutable.Seq[Out]`] @java[`In -> List<Out>`]. In this case we want to map a @scala[`Seq`] @java[`List`] of elements to the elements in the
collection itself, so we can call @scala[`mapConcat(identity)`] @java[`mapConcat(l -> l)`].

Scala
:   @@snip [RecipeFlattenSeq.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeFlattenSeq.scala) { #flattening-seqs }

Java
:   @@snip [RecipeFlattenList.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeFlattenList.java) { #flattening-lists }

### Draining a stream to a strict collection

**Situation:** A possibly unbounded sequence of elements is given as a stream, which needs to be collected into a Scala collection while ensuring boundedness

A common situation when working with streams is one where we need to collect incoming elements into a Scala collection.
This operation is supported via `Sink.seq` which materializes into a @scala[`Future[Seq[T]]`] @java[`CompletionStage<List<T>>`].

The function `limit` or `take` should always be used in conjunction in order to guarantee stream boundedness, thus preventing the program from running out of memory.

For example, this is best avoided:

Scala
:   @@snip [RecipeSeq.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeSeq.scala) { #draining-to-seq-unsafe }

Java
:   @@snip [RecipeSeq.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeSeq.java) { #draining-to-list-unsafe }

Rather, use `limit` or `take` to ensure that the resulting @scala[`Seq`] @java[`List`] will contain only up to @scala[`max`] @java[`MAX_ALLOWED_SIZE`] elements:

Scala
:   @@snip [RecipeSeq.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeSeq.scala) { #draining-to-seq-safe }

Java
:   @@snip [RecipeSeq.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeSeq.java) { #draining-to-list-safe }

### Calculating the digest of a ByteString stream

**Situation:** A stream of bytes is given as a stream of `ByteString` s and we want to calculate the cryptographic digest
of the stream.

This recipe uses a @ref[`GraphStage`](stream-customize.md) to define a custom Akka Stream operator, to host a mutable `MessageDigest` class (part of the Java Cryptography
API) and update it with the bytes arriving from the stream. When the stream starts, the `onPull` handler of the
operator is called, which bubbles up the `pull` event to its upstream. As a response to this pull, a ByteString
chunk will arrive (`onPush`) which we use to update the digest, then it will pull for the next chunk.

Eventually the stream of `ByteString` s depletes and we get a notification about this event via `onUpstreamFinish`.
At this point we want to emit the digest value, but we cannot do it with `push` in this handler directly since there may
be no downstream demand. Instead we call `emit` which will temporarily replace the handlers, emit the provided value when
demand comes in and then reset the operator state. It will then complete the operator.

Scala
:   @@snip [RecipeDigest.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeDigest.scala) { #calculating-digest }

Java
:   @@snip [RecipeDigest.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeDigest.java) { #calculating-digest }   
:   @@snip [RecipeDigest.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeDigest.java) { #calculating-digest2 }

<a id="cookbook-parse-lines"></a>
### Parsing lines from a stream of ByteStrings

**Situation:** A stream of bytes is given as a stream of `ByteString` s containing lines terminated by line ending
characters (or, alternatively, containing binary frames delimited by a special delimiter byte sequence) which
needs to be parsed.

The `Framing` helper @scala[object] @java[class] contains a convenience method to parse messages from a stream of `ByteString` s:

Scala
:   @@snip [RecipeParseLines.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeParseLines.scala) { #parse-lines }

Java
:   @@snip [RecipeParseLines.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeParseLines.java) { #parse-lines }

### Dealing with compressed data streams

**Situation:** A gzipped stream of bytes is given as a stream of `ByteString` s, for example from a `FileIO` source.

The `Compression` helper @scala[object] @java[class] contains convenience methods for decompressing data streams compressed with
Gzip or Deflate.

Scala
:   @@snip [RecipeDecompress.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeDecompress.scala) { #decompress-gzip }

Java
:   @@snip [RecipeDecompress.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeDecompress.java) { #decompress-gzip }

### Implementing a Splitter

**Situation:** Given a stream of messages, where each message is a composition of different elements, we want to split 
the message into a series of individual sub-messages, each of which may be processed in a different way.

The [Splitter](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) is an integration 
pattern as described in [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com). Let's say 
that we have a stream containing strings. Each string contains a few numbers separated by "-". We want to create out 
of this a stream that only contains the numbers.

Scala
: @@snip [RecipeSplitter.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeSplitter.scala) { #Simple-Split }

Java
: @@snip [RecipeSplitter.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeSplitter.java) { #Simple-Split }

### Implementing a Splitter and Aggregator

**Situation:** Given a message, we want to split the message and aggregate its sub-messages into a new message

Sometimes it's very useful to split a message and aggregate its sub-messages into a new message. This involves a 
combination of [Splitter](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) 
and [Aggregator](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html)

Let's say that now we want to create a new stream containing the sums of the numbers in each original string.


Scala
: @@snip [RecipeSplitter.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeSplitter.scala) { #Aggregate-Split }

Java
: @@snip [RecipeSplitter.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeSplitter.java) { #Aggregate-Split }

While in real life this solution is overkill for such a simple problem (you can just do everything in a map), 
more complex scenarios, involving in particular I/O, will benefit from the fact that you can parallelize sub-streams 
and get back-pressure for "free".

### Implementing reduce-by-key

**Situation:** Given a stream of elements, we want to calculate some aggregated value on different subgroups of the
elements.

The "hello world" of reduce-by-key style operations is *wordcount* which we demonstrate below. Given a stream of words
we first create a new stream that groups the words according to the @scala[`identity`] @java[`i -> i`] function, i.e. now
we have a stream of streams, where every substream will serve identical words.

To count the words, we need to process the stream of streams (the actual groups
containing identical words). `groupBy` returns a @scala[`SubFlow`] @java[`SubSource`], which
means that we transform the resulting substreams directly. In this case we use
the `reduce` operator to aggregate the word itself and the number of its
occurrences within a @scala[tuple `(String, Integer)`] @java[`Pair<String, Integer>`]. Each substream will then
emit one final value—precisely such a pair—when the overall input completes. As
a last step we merge back these values from the substreams into one single
output stream.

One noteworthy detail pertains to the @scala[`MaximumDistinctWords`] @java[`MAXIMUM_DISTINCT_WORDS`] parameter: this
defines the breadth of the groupBy and merge operations. Akka Streams is
focused on bounded resource consumption and the number of concurrently open
inputs to the merge operator describes the amount of resources needed by the
merge itself. Therefore only a finite number of substreams can be active at
any given time. If the `groupBy` operator encounters more keys than this
number then the stream cannot continue without violating its resource bound, in
this case `groupBy` will terminate with a failure.

Scala
:   @@snip [RecipeReduceByKey.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeReduceByKey.scala) { #word-count }

Java
:  @@snip [RecipeReduceByKeyTest.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeReduceByKeyTest.java) { #word-count }

By extracting the parts specific to *wordcount* into

 * a `groupKey` function that defines the groups
 * a `map` map each element to value that is used by the reduce on the substream
 * a `reduce` function that does the actual reduction

we get a generalized version below:

Scala
:   @@snip [RecipeReduceByKey.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeReduceByKey.scala) { #reduce-by-key-general }

Java
:   @@snip [RecipeReduceByKeyTest.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeReduceByKeyTest.java) { #reduce-by-key-general }
:   @@snip [RecipeReduceByKeyTest.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeReduceByKeyTest.java) { #reduce-by-key-general2 }


@@@ note

Please note that the reduce-by-key version we discussed above is sequential
in reading the overall input stream, in other words it is **NOT** a
parallelization pattern like MapReduce and similar frameworks.

@@@

### Sorting elements to multiple groups with groupBy

**Situation:** The `groupBy` operation strictly partitions incoming elements, each element belongs to exactly one group.
Sometimes we want to map elements into multiple groups simultaneously.

To achieve the desired result, we attack the problem in two steps:

 * first, using a function `topicMapper` that gives a list of topics (groups) a message belongs to, we transform our
stream of `Message` to a stream of @scala[`(Message, Topic)`] @java[`Pair<Message, Topic>`] where for each topic the message belongs to a separate pair
will be emitted. This is achieved by using `mapConcat`
 * Then we take this new stream of message topic pairs (containing a separate pair for each topic a given message
belongs to) and feed it into groupBy, using the topic as the group key.

Scala
:   @@snip [RecipeMultiGroupBy.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeMultiGroupBy.scala) { #multi-groupby }

Java
:   @@snip [RecipeMultiGroupByTest.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeMultiGroupByTest.java) { #multi-groupby }

### Adhoc source

**Situation:** The idea is that you have a source which you don't want to start until you have a demand.
Also, you want to shut it down when there is no more demand, and start it up again there is new demand again.

You can achieve this behavior by combining `lazySource`, `backpressureTimeout` and `recoverWithRetries` as follows:

Scala
:   @@snip [RecipeAdhocSource.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeAdhocSource.scala) { #adhoc-source }

Java
:   @@snip [RecipeAdhocSourceTest.scala](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeAdhocSourceTest.java) { #adhoc-source }


## Working with Operators

In this collection we show recipes that use stream operators to achieve various goals.

### Triggering the flow of elements programmatically

**Situation:** Given a stream of elements we want to control the emission of those elements according to a trigger signal.
In other words, even if the stream would be able to flow (not being backpressured) we want to hold back elements until a
trigger signal arrives.

This recipe solves the problem by zipping the stream of `Message` elements with the stream of `Trigger`
signals. Since `Zip` produces pairs, we map the output stream selecting the first element of the pair.

Scala
:   @@snip [RecipeManualTrigger.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeManualTrigger.scala) { #manually-triggered-stream }
    
Java
:   @@snip [RecipeManualTrigger.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeManualTrigger.java) { #manually-triggered-stream }

Alternatively, instead of using a `Zip`, and then using `map` to get the first element of the pairs, we can avoid
creating the pairs in the first place by using `ZipWith` which takes a two argument function to produce the output
element. If this function would return a pair of the two argument it would be exactly the behavior of `Zip` so
`ZipWith` is a generalization of zipping.

Scala
:   @@snip [RecipeManualTrigger.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeManualTrigger.scala) { #manually-triggered-stream-zipwith }
    
Java
:   @@snip [RecipeManualTrigger.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeManualTrigger.java) { #manually-triggered-stream-zipwith }

<a id="cookbook-balance"></a>
### Balancing jobs to a fixed pool of workers

**Situation:** Given a stream of jobs and a worker process expressed as a `Flow` create a pool of workers
that automatically balances incoming jobs to available workers, then merges the results.

We will express our solution as a function that takes a worker flow and the number of workers to be allocated and gives
a flow that internally contains a pool of these workers. To achieve the desired result we will create a `Flow`
from an operator.

The operator consists of a `Balance` node which is a special fan-out operation that tries to route elements to available
downstream consumers. In a `for` loop we wire all of our desired workers as outputs of this balancer element, then
we wire the outputs of these workers to a `Merge` element that will collect the results from the workers.

To make the worker operators run in parallel we mark them as asynchronous with *async*.

Scala
:   @@snip [RecipeWorkerPool.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeWorkerPool.scala) { #worker-pool }
    
Java
:   @@snip [RecipeWorkerPool.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeWorkerPool.java) { #worker-pool }
:   @@snip [RecipeWorkerPool.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeWorkerPool.java) { #worker-pool2 }

## Working with rate

This collection of recipes demonstrate various patterns where rate differences between upstream and downstream
needs to be handled by other strategies than simple backpressure.

### Dropping elements

**Situation:** Given a fast producer and a slow consumer, we want to drop elements if necessary to not slow down
the producer too much.

This can be solved by using a versatile rate-transforming operation, `conflate`. Conflate can be thought as
a special `reduce` operation that collapses multiple upstream elements into one aggregate element if needed to keep
the speed of the upstream unaffected by the downstream.

When the upstream is faster, the reducing process of the `conflate` starts. Our reducer function takes
the freshest element. This in a simple dropping operation.

Scala
:   @@snip [RecipeSimpleDrop.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeSimpleDrop.scala) { #simple-drop }
    
Java
:   @@snip [RecipeSimpleDrop.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeSimpleDrop.java) { #simple-drop }

There is a more general version of `conflate` named `conflateWithSeed` that allows to express more complex aggregations, more
similar to a `fold`.

### Dropping broadcast

**Situation:** The default `Broadcast` operator is properly backpressured, but that means that a slow downstream
consumer can hold back the other downstream consumers resulting in lowered throughput. In other words the rate of
`Broadcast` is the rate of its slowest downstream consumer. In certain cases it is desirable to allow faster consumers
to progress independently of their slower siblings by dropping elements if necessary.

One solution to this problem is to append a `buffer` element in front of all of the downstream consumers
defining a dropping strategy instead of the default `Backpressure`. This allows small temporary rate differences
between the different consumers (the buffer smooths out small rate variances), but also allows faster consumers to
progress by dropping from the buffer of the slow consumers if necessary.

Scala
:   @@snip [RecipeDroppyBroadcast.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeDroppyBroadcast.scala) { #droppy-bcast }
    
Java
:   @@snip [RecipeDroppyBroadcast.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeDroppyBroadcast.java) { #droppy-bcast }
:   @@snip [RecipeDroppyBroadcast.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeDroppyBroadcast.java) { #droppy-bcast2 }

### Collecting missed ticks

**Situation:** Given a regular (stream) source of ticks, instead of trying to backpressure the producer of the ticks
we want to keep a counter of the missed ticks instead and pass it down when possible.

We will use `conflateWithSeed` to solve the problem. The seed version of conflate takes two functions:

 * A seed function that produces the zero element for the folding process that happens when the upstream is faster than
the downstream. In our case the seed function is a constant function that returns 0 since there were no missed ticks
at that point.
 * A fold function that is invoked when multiple upstream messages needs to be collapsed to an aggregate value due
to the insufficient processing rate of the downstream. Our folding function increments the currently stored
count of the missed ticks so far.

As a result, we have a flow of `Int` where the number represents the missed ticks. A number 0 means that we were
able to consume the tick fast enough (i.e. zero means: 1 non-missed tick + 0 missed ticks)

Scala
:   @@snip [RecipeMissedTicks.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeMissedTicks.scala) { #missed-ticks }
    
Java
:   @@snip [RecipeMissedTicks.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeMissedTicks.java) { #missed-ticks }

### Create a stream processor that repeats the last element seen

**Situation:** Given a producer and consumer, where the rate of neither is known in advance, we want to ensure that none
of them is slowing down the other by dropping earlier unconsumed elements from the upstream if necessary, and repeating
the last value for the downstream if necessary.

We have two options to implement this feature. In both cases we will use @ref[`GraphStage`](stream-customize.md), to build our custom
operator. In the first version we will use a provided initial value `initial` that will be used
to feed the downstream if no upstream element is ready yet. In the `onPush()` handler we overwrite the
`currentValue` variable and immediately relieve the upstream by calling `pull()`. The downstream `onPull` handler
is very similar, we immediately relieve the downstream by emitting `currentValue`.

Scala
:   @@snip [RecipeHold.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeHold.scala) { #hold-version-1 }

Java
:   @@snip [RecipeHold.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeHold.java) { #hold-version-1 }

While it is relatively simple, the drawback of the first version is that it needs an arbitrary initial element which is not
always possible to provide. Hence, we create a second version where the downstream might need to wait in one single
case: if the very first element is not yet available.

We introduce a boolean variable `waitingFirstValue` to denote whether the first element has been provided or not
(alternatively an @scala[`Option`] @java[`Optional`] can be used for `currentValue` or if the element type is a subclass of @scala[`AnyRef`] @java[`Object`]
a null can be used with the same purpose). In the downstream `onPull()` handler the difference from the previous
version is that we check if we have received the first value and only emit if we have. This leads to that when the
first element comes in we must check if there possibly already was demand from downstream so that we in that case can
push the element directly.

Scala
:   @@snip [RecipeHold.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeHold.scala) { #hold-version-2 }
    
Java
:   @@snip [RecipeHold.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeHold.java) { #hold-version-2 }

### Globally limiting the rate of a set of streams

**Situation:** Given a set of independent streams that we cannot merge, we want to globally limit the aggregate
throughput of the set of streams.

One possible solution uses a shared actor as the global limiter combined with mapAsync to create a reusable
`Flow` that can be plugged into a stream to limit its rate.

As the first step we define an actor that will do the accounting for the global rate limit. The actor maintains
a timer, a counter for pending permit tokens and a queue for possibly waiting participants. The actor has
an `open` and `closed` state. The actor is in the `open` state while it has still pending permits. Whenever a
request for permit arrives as a `WantToPass` message to the actor the number of available permits is decremented
and we notify the sender that it can pass by answering with a `MayPass` message. If the amount of permits reaches
zero, the actor transitions to the `closed` state. In this state requests are not immediately answered, instead the reference
of the sender is added to a queue. Once the timer for replenishing the pending permits fires by sending a `ReplenishTokens`
message, we increment the pending permits counter and send a reply to each of the waiting senders. If there are more
waiting senders than permits available we will stay in the `closed` state.

Scala
:   @@snip [RecipeGlobalRateLimit.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeGlobalRateLimit.scala) { #global-limiter-actor }
    
Java
:   @@snip [RecipeGlobalRateLimit.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeGlobalRateLimit.java) { #global-limiter-actor }

To create a Flow that uses this global limiter actor we use the `mapAsync` function with the combination of the `ask`
pattern. We also define a timeout, so if a reply is not received during the configured maximum wait period the returned
future from `ask` will fail, which will fail the corresponding stream as well.

Scala
:   @@snip [RecipeGlobalRateLimit.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeGlobalRateLimit.scala) { #global-limiter-flow }
    
Java
:   @@snip [RecipeGlobalRateLimit.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeGlobalRateLimit.java) { #global-limiter-flow }

@@@ note

The global actor used for limiting introduces a global bottleneck. You might want to assign a dedicated dispatcher
for this actor.

@@@

## Working with IO

### Chunking up a stream of ByteStrings into limited size ByteStrings

**Situation:** Given a stream of `ByteString` s we want to produce a stream of `ByteString` s containing the same bytes in
the same sequence, but capping the size of `ByteString` s. In other words we want to slice up `ByteString` s into smaller
chunks if they exceed a size threshold.

This can be achieved with a single @ref[`GraphStage`](stream-customize.md) to define a custom Akka Stream operator. The main logic of our operator is in `emitChunk()`
which implements the following logic:

 * if the buffer is empty, and upstream is not closed we pull for more bytes, if it is closed we complete
 * if the buffer is nonEmpty, we split it according to the `chunkSize`. This will give a next chunk that we will emit,
and an empty or nonempty remaining buffer.

Both `onPush()` and `onPull()` calls `emitChunk()` the only difference is that the push handler also stores
the incoming chunk by appending to the end of the buffer.

Scala
:   @@snip [RecipeByteStrings.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeByteStrings.scala) { #bytestring-chunker }
    
Java
:   @@snip [RecipeByteStrings.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeByteStrings.java) { #bytestring-chunker }
:   @@snip [RecipeByteStrings.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeByteStrings.java) { #bytestring-chunker2 }

### Limit the number of bytes passing through a stream of ByteStrings

**Situation:** Given a stream of `ByteString` s we want to fail the stream if more than a given maximum of bytes has been
consumed.

This recipe uses a @ref[`GraphStage`](stream-customize.md) to implement the desired feature. In the only handler we override,
`onPush()` we update a counter and see if it gets larger than `maximumBytes`. If a violation happens
we signal failure, otherwise we forward the chunk we have received.

Scala
:   @@snip [RecipeByteStrings.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeByteStrings.scala) { #bytes-limiter }
    
Java
:   @@snip [RecipeByteStrings.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeByteStrings.java) { #bytes-limiter }
    @@snip [RecipeByteStrings.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeByteStrings.java) { #bytes-limiter2 }
        
### Compact ByteStrings in a stream of ByteStrings

**Situation:** After a long stream of transformations, due to their immutable, structural sharing nature `ByteString` s may
refer to multiple original ByteString instances unnecessarily retaining memory. As the final step of a transformation
chain we want to have clean copies that are no longer referencing the original `ByteString` s.

The recipe is a simple use of map, calling the `compact()` method of the `ByteString` elements. This does
copying of the underlying arrays, so this should be the last element of a long chain if used.

Scala
:   @@snip [RecipeByteStrings.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeByteStrings.scala) { #compacting-bytestrings }
    
Java
:   @@snip [RecipeByteStrings.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeByteStrings.java) { #compacting-bytestrings }
    
### Injecting keep-alive messages into a stream of ByteStrings

**Situation:** Given a communication channel expressed as a stream of `ByteString` s we want to inject keep-alive messages
but only if this does not interfere with normal traffic.

There is a built-in operation that allows to do this directly:

Scala
:   @@snip [RecipeKeepAlive.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeKeepAlive.scala) { #inject-keepalive }

Java
:   @@snip [RecipeKeepAlive.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeKeepAlive.java) { #inject-keepalive }
