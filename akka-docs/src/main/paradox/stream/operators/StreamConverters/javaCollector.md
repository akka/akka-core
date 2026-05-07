# StreamConverters.javaCollector

Create a sink which materializes into a @scala[`Future`] @java[`CompletionStage`] which will be completed with a result of the Java 8 `Collector` transformation and reduction operations.

@ref[Additional Sink and Source converters](../index.md#additional-sink-and-source-converters)

## Signature

@apidoc[StreamConverters.javaCollector](StreamConverters$) { scala="#javaCollector[T,R](collectorFactory:()=&gt;java.util.stream.Collector[T,_,R]):akka.stream.scaladsl.Sink[T,scala.concurrent.Future[R]]" java="#javaCollector(akka.japi.function.Creator)" }


## Description

TODO: We would welcome help on contributing descriptions and examples, see: https://github.com/akka/akka-core/issues/25646
