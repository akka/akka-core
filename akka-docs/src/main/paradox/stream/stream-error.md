# Error Handling in Streams

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

When an operator in a stream fails this will normally lead to the entire stream being torn down.
Each of the operators downstream gets informed about the failure and each upstream operator sees a cancellation.

In many cases you may want to avoid complete stream failure, this can be done in a few different ways:

 * @apidoc[recover](akka.stream.*.Source) {scala="#recover[T&gt;:Out](pf:PartialFunction[Throwable,T]):FlowOps.this.Repr[T]" java="#recover(java.lang.Class,java.util.function.Supplier)"} to emit a final element then complete the stream normally on upstream failure 
 * @apidoc[recoverWithRetries](akka.stream.*.Source) {scala="#recoverWithRetries[T&gt;:Out](attempts:Int,pf:PartialFunction[Throwable,akka.stream.Graph[akka.stream.SourceShape[T],akka.NotUsed]]):FlowOps.this.Repr[T]" java="#recoverWithRetries(int,java.lang.Class,java.util.function.Supplier)"} to create a new upstream and start consuming from that on failure
 * Restarting sections of the stream after a backoff
 * Using a supervision strategy for operators that support it
 
In addition to these built in tools for error handling, a common pattern is to wrap the stream 
inside an actor, and have the actor restart the entire stream on failure.
 
## Logging errors

@apidoc[log()](akka.stream.*.Source) {scala="#log(name:String,extract:Out=%3EAny)(implicitlog:akka.event.LoggingAdapter):FlowOps.this.Repr[Out]" java="#log(java.lang.String,akka.japi.function.Function)"} enables logging of a stream, which is typically useful for error logging. 
The below stream fails with @javadoc[ArithmeticException](java.lang.ArithmeticException) when the element `0` goes through the @apidoc[map](akka.stream.*.Source) {scala="#map[T](f:Out=%3ET):FlowOps.this.Repr[T]" java="#map(akka.japi.function.Function)"} operator, 

Scala
:   @@snip [RecipeLoggingElements.scala](/akka-docs/src/test/scala/docs/stream/cookbook/RecipeLoggingElements.scala) { #log-error }

Java
:   @@snip [RecipeLoggingElements.java](/akka-docs/src/test/java/jdocs/stream/javadsl/cookbook/RecipeLoggingElements.java) { #log-error }


and error messages like below will be logged. 

```
[error logging] Upstream failed.
java.lang.ArithmeticException: / by zero
```

If you want to control logging levels on each element, completion, and failure, you can find more details 
in @ref:[Logging in streams](stream-cookbook.md#logging-in-streams).

## Recover

@apidoc[recover](akka.stream.*.Source) {scala="#recover[T&gt;:Out](pf:PartialFunction[Throwable,T]):FlowOps.this.Repr[T]" java="#recover(java.lang.Class,java.util.function.Supplier)"} allows you to emit a final element and then complete the stream on an upstream failure.
Deciding which exceptions should be recovered is done through a @scaladoc[PartialFunction](scala.PartialFunction). If an exception
does not have a @scala[matching case] @java[match defined] the stream is failed. 

Recovering can be useful if you want to gracefully complete a stream on failure while letting 
downstream know that there was a failure.

Throwing an exception inside `recover` _will_ be logged on ERROR level automatically.

More details in @ref[recover](./operators/Source-or-Flow/recover.md#recover)

Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #recover }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #recover }

This will output:

Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #recover-output }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #recover-output }


## Recover with retries

@apidoc[recoverWithRetries](akka.stream.*.Source) {scala="#recoverWithRetries[T&gt;:Out](attempts:Int,pf:PartialFunction[Throwable,akka.stream.Graph[akka.stream.SourceShape[T],akka.NotUsed]]):FlowOps.this.Repr[T]" java="#recoverWithRetries(int,java.lang.Class,java.util.function.Supplier)"} allows you to put a new upstream in place of the failed one, recovering 
stream failures up to a specified maximum number of times. 

Deciding which exceptions should be recovered is done through a @scaladoc[PartialFunction](scala.PartialFunction). If an exception
does not have a @scala[matching case] @java[match defined] the stream is failed.

Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #recoverWithRetries }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #recoverWithRetries }

This will output:


Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #recoverWithRetries-output }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #recoverWithRetries-output }


<a id="restart-with-backoff"></a>

## Delayed restarts with a backoff operator

Akka streams provides a @apidoc[akka.stream.*.RestartSource$], @apidoc[akka.stream.*.RestartSink$] and @apidoc[akka.stream.*.RestartFlow$] for implementing the so-called *exponential backoff 
supervision strategy*, starting an operator again when it fails or completes, each time with a growing time delay between restarts.

This pattern is useful when the operator fails or completes because some external resource is not available
and we need to give it some time to start-up again. One of the prime examples when this is useful is
when a WebSocket connection fails due to the HTTP server it's running on going down, perhaps because it is overloaded. 
By using an exponential backoff, we avoid going into a tight reconnect loop, which both gives the HTTP server some time
to recover, and it avoids using needless resources on the client side.

The various restart shapes mentioned all expect an @apidoc[akka.stream.RestartSettings] which configures the restart behaviour.
Configurable parameters are:

* `minBackoff` is the initial duration until the underlying stream is restarted
* `maxBackoff` caps the exponential backoff
* `randomFactor` allows addition of a random delay following backoff calculation
* `maxRestarts` caps the total number of restarts
* `maxRestartsWithin` sets a timeframe during which restarts are counted towards the same total for `maxRestarts`

The following snippet shows how to create a backoff supervisor using @apidoc[akka.stream.*.RestartSource$] 
which will supervise the given @apidoc[akka.stream.*.Source]. The `Source` in this case is a 
stream of Server Sent Events, produced by akka-http. If the stream fails or completes at any point, the request will
be made again, in increasing intervals of 3, 6, 12, 24 and finally 30 seconds (at which point it will remain capped due
to the `maxBackoff` parameter):

Scala
:   @@snip [RestartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RestartDocSpec.scala) { #restart-with-backoff-source }

Java
:   @@snip [RestartDocTest.java](/akka-docs/src/test/java/jdocs/stream/RestartDocTest.java) { #restart-with-backoff-source }

Using a `randomFactor` to add a little bit of additional variance to the backoff intervals
is highly recommended, in order to avoid multiple streams re-start at the exact same point in time,
for example because they were stopped due to a shared resource such as the same server going down
and re-starting after the same configured interval. By adding additional randomness to the
re-start intervals the streams will start in slightly different points in time, thus avoiding
large spikes of traffic hitting the recovering server or other resource that they all need to contact.

The above `RestartSource` will never terminate unless the @apidoc[akka.stream.*.Sink] it's fed into cancels. It will often be handy to use
it in combination with a @ref:[`KillSwitch`](stream-dynamic.md#kill-switch), so that you can terminate it when needed:

Scala
:   @@snip [RestartDocSpec.scala](/akka-docs/src/test/scala/docs/stream/RestartDocSpec.scala) { #with-kill-switch }

Java
:   @@snip [RestartDocTest.java](/akka-docs/src/test/java/jdocs/stream/RestartDocTest.java) { #with-kill-switch }

Sinks and flows can also be supervised, using @apidoc[akka.stream.*.RestartSink$] and @apidoc[akka.stream.*.RestartFlow$]. The `RestartSink` is restarted when
it cancels, while the `RestartFlow` is restarted when either the in port cancels, the out port completes, or the out
 port sends an error.

@@@ note

Care should be taken when using @ref[`GraphStage`s](stream-customize.md) that conditionally propagate termination signals inside a
@apidoc[akka.stream.*.RestartSource$], @apidoc[akka.stream.*.RestartSink$] or @apidoc[akka.stream.*.RestartFlow$].  

An example is a @scaladoc[Broadcast](akka.stream.scaladsl.Broadcast) operator with the default `eagerCancel = false` where 
some of the outlets are for side-effecting branches (that do not re-join e.g. via a `Merge`). 
A failure on a side branch will not terminate the supervised stream which will 
not be restarted. Conversely, a failure on the main branch can trigger a restart but leave behind old
running instances of side branches.

In this example `eagerCancel` should probably be set to `true`, or, when only a single side branch is used, @ref[`alsoTo`](operators/Source-or-Flow/alsoTo.md)
or @ref[`divertTo`](operators/Source-or-Flow/divertTo.md) should be considered as alternatives.

@@@

## Supervision Strategies

@@@ note

The operators that support supervision strategies are explicitly documented to do so, if there is
nothing in the documentation of an operator saying that it adheres to the supervision strategy it
means it fails rather than applies supervision.

@@@

The error handling strategies are inspired by actor supervision strategies, but the semantics 
have been adapted to the domain of stream processing. The most important difference is that 
supervision is not automatically applied to stream operators but instead something that each operator 
has to implement explicitly. 

For many operators it may not even make sense to implement support for supervision strategies,
this is especially true for operators connecting to external technologies where for example a
failed connection will likely still fail if a new connection is tried immediately (see 
@ref:[Restart with back off](#restart-with-backoff) for such scenarios). 

For operators that do implement supervision, the strategies for how to handle exceptions from 
processing stream elements can be selected when materializing the stream through use of an attribute. 

There are three ways to handle exceptions from application code:

 * @scala[@scaladoc[Stop](akka.stream.Supervision$$Stop$)]@java[@javadoc[Supervision.stop()](akka.stream.Supervision#stop())] - The stream is completed with failure.
 * @scala[@scaladoc[Resume](akka.stream.Supervision$$Resume$)]@java[@javadoc[Supervision.resume()](akka.stream.Supervision#resume())] - The element is dropped and the stream continues.
 * @scala[@scaladoc[Restart](akka.stream.Supervision$$Restart$)]@java[@javadoc[Supervision.restart()](akka.stream.Supervision#restart())] - The element is dropped and the stream continues after restarting the operator.
Restarting an operator means that any accumulated state is cleared. This is typically
performed by creating a new instance of the operator.

By default the stopping strategy is used for all exceptions, i.e. the stream will be completed with
failure when an exception is thrown.

Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #stop }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #stop }

The default supervision strategy for a stream can be defined on the complete @apidoc[akka.stream.*.RunnableGraph].

Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #resume }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #resume }

Here you can see that all @javadoc[ArithmeticException](java.lang.ArithmeticException) will resume the processing, i.e. the
elements that cause the division by zero are effectively dropped.

@@@ note

Be aware that dropping elements may result in deadlocks in graphs with
cycles, as explained in @ref:[Graph cycles, liveness and deadlocks](stream-graphs.md#graph-cycles).

@@@

The supervision strategy can also be defined for all operators of a flow.

Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #resume-section }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #resume-section }

@scala[@scaladoc[Restart](akka.stream.Supervision$$Restart$)]@java[@javadoc[Supervision.restart()](akka.stream.Supervision#restart())] works in a similar way as @scala[@scaladoc[Resume](akka.stream.Supervision$$Resume$)]@java[@javadoc[Supervision.resume()](akka.stream.Supervision#resume())] with the addition that accumulated state,
if any, of the failing processing operator will be reset.

Scala
:   @@snip [FlowErrorDocSpec.scala](/akka-docs/src/test/scala/docs/stream/FlowErrorDocSpec.scala) { #restart-section }

Java
:   @@snip [FlowErrorDocTest.java](/akka-docs/src/test/java/jdocs/stream/FlowErrorDocTest.java) { #restart-section }

### Errors from mapAsync

Stream supervision can also be applied to the futures of @apidoc[mapAsync](akka.stream.*.Source) {scala="#mapAsync[T](parallelism:Int)(f:Out=%3Escala.concurrent.Future[T]):FlowOps.this.Repr[T]" java="#mapAsync(int,akka.japi.function.Function)"} and @apidoc[mapAsyncUnordered](akka.stream.*.Source) {scala="#mapAsyncUnordered[T](parallelism:Int)(f:Out=%3Escala.concurrent.Future[T]):FlowOps.this.Repr[T]" java="#mapAsyncUnordered(int,akka.japi.function.Function)"} even if such
failures happen in the future rather than inside the operator itself.

Let's say that we use an external service to lookup email addresses and we would like to
discard those that cannot be found.

We start with the tweet stream of authors:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #tweet-authors }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #tweet-authors }

Assume that we can lookup their email address using:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #email-address-lookup2 }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #email-address-lookup2 }

The @scala[@scaladoc[Future](scala.concurrent.Future)] @java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] is completed @scala[with `Failure`] @java[normally] if the email is not found.

Transforming the stream of authors to a stream of email addresses by using the `lookupEmail`
service can be done with @apidoc[mapAsync](akka.stream.*.Source) {scala="#mapAsync[T](parallelism:Int)(f:Out=%3Escala.concurrent.Future[T]):FlowOps.this.Repr[T]" java="#mapAsync(int,akka.japi.function.Function)"} and we use @scala[@scaladoc[Supervision.resumingDecider](akka.stream.Supervision$#resumingDecider:akka.stream.Supervision.Deciderwithakka.japi.function.Function[Throwable,akka.stream.Supervision.Directive])] @java[@javadoc[Supervision.getResumingDecider()](akka.stream.Supervision#getResumingDecider())] to drop
unknown email addresses:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #email-addresses-mapAsync-supervision }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #email-addresses-mapAsync-supervision }

If we would not use @scala[@scaladoc[Resume](akka.stream.Supervision$$Resume$)]@java[@javadoc[Supervision.resume()](akka.stream.Supervision#resume())] the default stopping strategy would complete the stream
with failure on the first @scala[`Future`] @java[`CompletionStage`] that was completed @scala[with `Failure`]@java[exceptionally].

