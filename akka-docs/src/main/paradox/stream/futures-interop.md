# Futures interop

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

## Overview

Stream transformations and side effects involving external non-stream based services can be
performed with `mapAsync` or `mapAsyncUnordered`.

For example, sending emails to the authors of selected tweets using an external
email service:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #email-server-send }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #email-server-send }

We start with the tweet stream of authors:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #tweet-authors}

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #tweet-authors }

Assume that we can look up their email address using:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #email-address-lookup }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #email-address-lookup }

Transforming the stream of authors to a stream of email addresses by using the `lookupEmail`
service can be done with `mapAsync`:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #email-addresses-mapAsync }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #email-addresses-mapAsync }

Finally, sending the emails:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #send-emails }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #send-emails }

`mapAsync` is applying the given function that is calling out to the external service to
each of the elements as they pass through this processing step. The function returns a @scala[`Future`]@java[`CompletionStage`]
and the value of that future will be emitted downstream. The number of Futures
that shall run in parallel is given as the first argument to `mapAsync`.
These Futures may complete in any order, but the elements that are emitted
downstream are in the same order as received from upstream.

That means that back-pressure works as expected. For example if the `emailServer.send`
is the bottleneck it will limit the rate at which incoming tweets are retrieved and
email addresses looked up.

The final piece of this pipeline is to generate the demand that pulls the tweet
authors information through the emailing pipeline: we attach a `Sink.ignore`
which makes it all run. If our email process would return some interesting data
for further transformation then we would not ignore it but send that
result stream onwards for further processing or storage.

Note that `mapAsync` preserves the order of the stream elements. In this example the order
is not important and then we can use the more efficient `mapAsyncUnordered`:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #external-service-mapAsyncUnordered }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #external-service-mapAsyncUnordered }

In the above example the services conveniently returned a  @scala[`Future`]@java[`CompletionStage`] of the result.
If that is not the case you need to wrap the call in a  @scala[`Future`]@java[`CompletionStage`]. If the service call
involves blocking you must also make sure that you run it on a dedicated execution context, to
avoid starvation and disturbance of other tasks in the system.

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #blocking-mapAsync }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #blocking-mapAsync }

The configuration of the `"blocking-dispatcher"` may look something like:

@@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #blocking-dispatcher-config }

An alternative for blocking calls is to perform them in a `map` operation, still using a
dedicated dispatcher for that operation.

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #blocking-map }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #blocking-map }

However, that is not exactly the same as `mapAsync`, since the `mapAsync` may run
several calls concurrently, but `map` performs them one at a time.

For a service that is exposed as an actor, or if an actor is used as a gateway in front of an
external service, you can use `ask`:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #save-tweets }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #save-tweets }

Note that if the `ask` is not completed within the given timeout the stream is completed with failure.
If that is not desired outcome you can use `recover` on the `ask` @scala[`Future`]@java[`CompletionStage`].

### Illustrating ordering and parallelism

Let us look at another example to get a better understanding of the ordering
and parallelism characteristics of `mapAsync` and `mapAsyncUnordered`.

Several `mapAsync` and `mapAsyncUnordered` futures may run concurrently.
The number of concurrent futures are limited by the downstream demand.
For example, if 5 elements have been requested by downstream there will be at most 5
futures in progress.

`mapAsync` emits the future results in the same order as the input elements
were received. That means that completed results are only emitted downstream
when earlier results have been completed and emitted. One slow call will thereby
delay the results of all successive calls, even though they are completed before
the slow call.

`mapAsyncUnordered` emits the future results as soon as they are completed, i.e.
it is possible that the elements are not emitted downstream in the same order as
received from upstream. One slow call will thereby not delay the results of faster
successive calls as long as there is downstream demand of several elements.

Here is a fictive service that we can use to illustrate these aspects.

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #sometimes-slow-service }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #sometimes-slow-service }

Elements starting with a lower case character are simulated to take longer time
to process.

Here is how we can use it with `mapAsync`:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #sometimes-slow-mapAsync }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #sometimes-slow-mapAsync }

The output may look like this:

```
before: a
before: B
before: C
before: D
running: a (1)
running: B (2)
before: e
running: C (3)
before: F
running: D (4)
before: g
before: H
completed: C (3)
completed: B (2)
completed: D (1)
completed: a (0)
after: A
after: B
running: e (1)
after: C
after: D
running: F (2)
before: i
before: J
running: g (3)
running: H (4)
completed: H (2)
completed: F (3)
completed: e (1)
completed: g (0)
after: E
after: F
running: i (1)
after: G
after: H
running: J (2)
completed: J (1)
completed: i (0)
after: I
after: J
```

Note that `after` lines are in the same order as the `before` lines even
though elements are `completed` in a different order. For example `H`
is `completed` before `g`, but still emitted afterwards.

The numbers in parentheses illustrate how many calls that are in progress at
the same time. Here the downstream demand and thereby the number of concurrent
calls are limited by the buffer size (4) set with an attribute.

Here is how we can use the same service with `mapAsyncUnordered`:

Scala
:   @@snip [IntegrationDocSpec.scala](/akka-docs/src/test/scala/docs/stream/IntegrationDocSpec.scala) { #sometimes-slow-mapAsyncUnordered }

Java
:   @@snip [IntegrationDocTest.java](/akka-docs/src/test/java/jdocs/stream/IntegrationDocTest.java) { #sometimes-slow-mapAsyncUnordered }

The output may look like this:

```
before: a
before: B
before: C
before: D
running: a (1)
running: B (2)
before: e
running: C (3)
before: F
running: D (4)
before: g
before: H
completed: B (3)
completed: C (1)
completed: D (2)
after: B
after: D
running: e (2)
after: C
running: F (3)
before: i
before: J
completed: F (2)
after: F
running: g (3)
running: H (4)
completed: H (3)
after: H
completed: a (2)
after: A
running: i (3)
running: J (4)
completed: J (3)
after: J
completed: e (2)
after: E
completed: g (1)
after: G
completed: i (0)
after: I
```

Note that `after` lines are not in the same order as the `before` lines. For example
`H` overtakes the slow `G`.

The numbers in parentheses illustrate how many calls that are in progress at
the same time. Here the downstream demand and thereby the number of concurrent
calls are limited by the buffer size (4) set with an attribute.
