# Source.unfoldResourceAsync

Wrap any resource that can be opened, queried for next element and closed in an asynchronous way.

@ref[Source operators](../index.md#source-operators)

## Signature

@apidoc[Source.unfoldResourceAsync](Source$) { scala="#unfoldResourceAsync[T,S](create:()=&gt;scala.concurrent.Future[S],read:S=&gt;scala.concurrent.Future[Option[T]],close:S=&gt;scala.concurrent.Future[akka.Done]):akka.stream.scaladsl.Source[T,akka.NotUsed]" java="#unfoldResourceAsync(akka.japi.function.Creator,akka.japi.function.Function,akka.japi.function.Function)" }


## Description

Wrap any resource that can be opened, queried for next element and closed in an asynchronous way with three distinct functions into a source. This operator is the equivalent of @ref[unfoldResource](unfoldResource.md) but for resources with asynchronous APIs.

`Source.unfoldResourceAsync` allows us to safely extract stream elements from a resource with an async API by providing it with 
three functions that all return a @scala[`Future`]@java[`CompletionStage`]: 

1. `create`: Open or create the resource
1. `read`: Fetch the next element or signal that we reached the end of the stream by completing the @scala[`Future`]@java[`CompletionStage`] with a @java[`Optional.empty`]@scala[`None`]
1. `close`: Close the resource, invoked on end of stream or if the stream fails

All exceptions thrown by `create` and `close` as well as the @scala[`Future`]@java[`CompletionStage`]s completing with failure will
fail the stream. The supervision strategy is used to handle exceptions from `read`, `create` and from the @scala[`Future`]@java[`CompletionStage`]s.

Note that there are pre-built `unfoldResourceAsync`-like operators to wrap `java.io.InputStream`s in 
@ref:[Additional Sink and Source converters](../index.md#additional-sink-and-source-converters), 
`Iterator` in @ref:[fromIterator](fromIterator.md) and File IO in @ref:[File IO Sinks and Sources](../index.md#file-io-sinks-and-sources).
Additional prebuilt technology specific connectors can also be found in the [Alpakka project](https://doc.akka.io/libraries/alpakka/current/).

## Examples

Imagine we have an async database API which we initially perform an async query and then can
check if there are more results in an asynchronous way.

Scala
:   @@snip [UnfoldResourceAsync.scala](/akka-docs/src/test/scala/docs/stream/operators/source/UnfoldResourceAsync.scala) { #unfoldResource-async-api }

Java
:   @@snip [UnfoldResourceAsync.java](/akka-docs/src/test/java/jdocs/stream/operators/source/UnfoldResourceAsync.java) { #unfoldResource-async-api }

Let's see how we use the API above safely through `unfoldResourceAsync`:

Scala
:   @@snip [UnfoldResourceAsync.scala](/akka-docs/src/test/scala/docs/stream/operators/source/UnfoldResourceAsync.scala) { #unfoldResourceAsync }

Java
:   @@snip [UnfoldResource.java](/akka-docs/src/test/java/jdocs/stream/operators/source/UnfoldResourceAsync.java) { #unfoldResourceAsync }

## Reactive Streams semantics

@@@div { .callout }

**emits** when there is demand and @scala[`Future`] @java[`CompletionStage`] from read function returns value

**completes** when @scala[`Future`] @java[`CompletionStage`] from read function returns `None`

@@@
