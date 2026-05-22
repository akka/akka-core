# StreamConverters.javaCollector

Create a sink which materializes into a @scala[`Future`] @java[`CompletionStage`] which will be completed with a result of the Java 8 `Collector` transformation and reduction operations.

@ref[Additional Sink and Source converters](../index.md#additional-sink-and-source-converters)

## Signature

@apidoc[StreamConverters.javaCollector](StreamConverters$) { scala="#javaCollector[T,R](collectorFactory:()=&gt;java.util.stream.Collector[T,_,R]):akka.stream.scaladsl.Sink[T,scala.concurrent.Future[R]]" java="#javaCollector(akka.japi.function.Creator)" }


## Description

Creates a @apidoc[Sink] that materializes into a @scala[`Future`]@java[`CompletionStage`] containing the
result of applying a Java 8 @javadoc[java.util.stream.Collector](java.util.stream.Collector) to the incoming
stream elements. The ``Collector`` will trigger demand downstream and will accumulate elements into a mutable
result container, with an optional finisher transformation after all elements have been processed. Reduction
processing is performed sequentially.

Note that a sink can be materialized multiple times, so the ``collectorFactory`` must create a
fresh ``Collector`` for each materialization.

See also @ref:[javaCollectorParallelUnordered](javaCollectorParallelUnordered.md) for a parallel version,
and @ref:[`Sink.collect`](../Sink/collect.md) for a convenience wrapper.

## Example

In this example, we use `StreamConverters.javaCollector` with ``Collectors.toList`` to collect
a stream of strings into a ``List``.

Scala
:   @@snip [JavaCollectorDocExample.scala](/akka-docs/src/test/scala/docs/stream/operators/JavaCollectorDocExample.scala) { #javaCollector }

Java
:   @@snip [JavaCollectorDocExamples.java](/akka-docs/src/test/java/jdocs/stream/operators/JavaCollectorDocExamples.java) { #javaCollector }
