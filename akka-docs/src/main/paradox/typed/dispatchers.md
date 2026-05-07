---
project.description: Akka dispatchers and how to choose the right ones.
---
# Dispatchers

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Dispatchers](../dispatchers.md).

## Dependency

Dispatchers are part of core Akka, which means that they are part of the `akka-actor` dependency. This
page describes how to use dispatchers with `akka-actor-typed`.

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor-typed_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction 

An Akka `MessageDispatcher` is what makes Akka Actors "tick", it is the engine of the machine so to speak.
All `MessageDispatcher` implementations are also an @scala[`ExecutionContext`]@java[`Executor`], which means that they can be used
to execute arbitrary code, for instance @scala[`Future`s]@java[`CompletableFuture`s].

## Default dispatcher

Every `ActorSystem` will have a default dispatcher that will be used in case nothing else is configured for an `Actor`.
The default dispatcher can be configured, and is by default a `Dispatcher` with the configured `akka.actor.default-dispatcher.executor`.
If no executor is selected a "fork-join-executor" is selected, which
gives excellent performance in most cases.

## Internal dispatcher

To protect the internal Actors that are spawned by the various Akka modules, a separate internal dispatcher is used by default.
The internal dispatcher can be tuned in a fine-grained way with the setting `akka.actor.internal-dispatcher`, it can also
be replaced by another dispatcher by making `akka.actor.internal-dispatcher` an @ref[alias](#dispatcher-aliases).

<a id="dispatcher-lookup"></a>
## Looking up a Dispatcher

Dispatchers implement the @scala[`ExecutionContext`]@java[`Executor`] interface and can thus be used to run @scala[`Future`]@java[`CompletableFuture`] invocations etc.

Scala
:  @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/actor/typed/DispatcherDocSpec.scala) { #lookup }

Java
:  @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/actor/typed/DispatcherDocTest.java) { #lookup }

## Selecting a dispatcher

A default dispatcher is used for all actors that are spawned without specifying a custom dispatcher.
This is suitable for all actors that don't block. Blocking in actors needs to be carefully managed, more
details @ref:[here](#blocking-needs-careful-management).

To select a dispatcher use `DispatcherSelector` to create a `Props` instance for spawning your actor:

Scala
:  @@snip [DispatcherDocSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/DispatchersDocSpec.scala) { #spawn-dispatcher }

Java
:  @@snip [DispatcherDocTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/DispatchersDocTest.java) { #spawn-dispatcher }

`DispatcherSelector` has a few convenience methods:

* @scala[`DispatcherSelector.default`]@java[`DispatcherSelector.defaultDispatcher`] to look up the default dispatcher
* `DispatcherSelector.blocking` can be used to execute actors that block e.g. a legacy database API that does not support @scala[`Future`]@java[`CompletionStage`]s
* `DispatcherSelector.sameAsParent` to use the same dispatcher as the parent actor

The final example shows how to load a custom dispatcher from configuration and relies on this being in your `application.conf`:

<!-- Same between Java and Scala -->
@@snip [DispatcherDocSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/DispatchersDocSpec.scala) { #config }

## Types of dispatchers

There are 2 different types of message dispatchers:

* **Dispatcher**

    This is an event-based dispatcher that binds a set of Actors to a thread
    pool. The default dispatcher is used if no other is specified.

    * Shareability: Unlimited
    * Mailboxes: Any, creates one per Actor
    * Use cases: Default dispatcher, Bulkheading
    * Driven by: `java.util.concurrent.ExecutorService`.
      Specify using "executor" using "fork-join-executor", "thread-pool-executor" or the fully-qualified
      class name of an `akka.dispatcher.ExecutorServiceConfigurator` implementation.

* **PinnedDispatcher**

    This dispatcher dedicates a unique thread for each actor using it; i.e.
    each actor will have its own thread pool with only one thread in the pool.

    * Shareability: None
    * Mailboxes: Any, creates one per Actor
    * Use cases: Bulkheading
    * Driven by: Any `akka.dispatch.ThreadPoolExecutorConfigurator`.
      By default a "thread-pool-executor".

Here is an example configuration of a Fork Join Pool dispatcher:

<!--same config text for Scala & Java-->
@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #my-dispatcher-config }

For more configuration options, see the @ref:[More dispatcher configuration examples](#more-dispatcher-configuration-examples)
section and the `default-dispatcher` section of the @ref:[configuration](../general/configuration.md).

@@@ note

The `parallelism-max` for the `fork-join-executor` does not set the upper bound on the total number of threads
allocated by the ForkJoinPool. It is a setting specifically talking about the number of *hot*
threads the pool will keep running in order to reduce the latency of handling a new incoming task.  Threads may use
`ManagedBlocker` (used by (among others) @scala[`Await` and `blocking {}`]@java[`CompletableFuture::get` and `CompletableFuture::join`])
to signal the pool that it might be desirable to add a thread (note that @ref:[this is not really a solution](#non-solution-)).
Prior to Akka 2.10.7, dispatchers with a `fork-join-executor` did not meaningfully bound the number of the additional threads which might
be added.  From Akka 2.10.7 onwards, the default if `maximum-spare-threads` is not set in config is "no meaningful bound", but a limit
can be set.  A future (at least 2.11) version of Akka may change this default behavior.

You can read more about parallelism in the JDK's [ForkJoinPool documentation](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/ForkJoinPool.html).

@@@

@@@ note

The `thread-pool-executor` dispatcher is implemented using by a `java.util.concurrent.ThreadPoolExecutor`.
You can read more about it in the JDK's [ThreadPoolExecutor documentation](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ThreadPoolExecutor.html).

@@@

## Dispatcher aliases

When a dispatcher is looked up, and the given setting contains a string rather than a dispatcher config block,
the lookup will treat it as an alias, and follow that string to an alternate location for a dispatcher config.
If the dispatcher config is referenced both through an alias and through the absolute path only one dispatcher will
be used and shared among the two ids.

Example: configuring `internal-dispatcher` to be an alias for `default-dispatcher`:

```
akka.actor.internal-dispatcher = akka.actor.default-dispatcher
```

<a id="blocking-management"></a>
## Blocking Needs Careful Management

In some cases it is unavoidable to do blocking operations, i.e. to put a thread
to sleep for an indeterminate time, waiting for an external event to occur.
Examples are legacy RDBMS drivers or messaging APIs, and the underlying reason
is typically that (network) I/O occurs under the covers.

The [Managing Blocking in Akka video](https://akka.io/blog/news/2020/01/22/managing-blocking-video)
explains why it is bad to block inside an actor, and how you can use custom dispatchers to manage
blocking when you cannot avoid it.

### Problem: Blocking on default dispatcher

Simply adding blocking calls to your actor message processing like this is problematic:

Scala
:   @@snip [BlockingDispatcherSample.scala](/akka-docs/src/test/scala/docs/actor/typed/BlockingActor.scala) { #blocking-in-actor }

Java
:   @@snip [BlockingActor.java](/akka-docs/src/test/java/jdocs/actor/typed/BlockingActor.java) { #blocking-in-actor }

Without any further configuration the default dispatcher runs this actor along
with all other actors. This is very efficient when all actor message processing is
non-blocking. When all of the available threads are blocked, however, then all the actors on the same dispatcher will starve for threads and
will not be able to process incoming messages.

@@@ note

Blocking APIs should also be avoided if possible. Try to find or build Reactive APIs,
such that blocking is minimised, or moved over to dedicated dispatchers.

Often when integrating with existing libraries or systems it is not possible to
avoid blocking APIs. The following solution explains how to handle blocking
operations properly.

Note that the same hints apply to managing blocking operations anywhere in Akka,
including Streams, HTTP and other reactive libraries built on top of it.

@@@

To demonstrate this problem, let's set up an application with the above `BlockingActor` and the following `PrintActor`:

Scala
:   @@snip [PrintActor.scala](/akka-docs/src/test/scala/docs/actor/typed/PrintActor.scala) { #print-actor }

Java
:   @@snip [PrintActor.java](/akka-docs/src/test/java/jdocs/actor/typed/PrintActor.java) { #print-actor }


Scala
:   @@snip [BlockingDispatcherSample.scala](/akka-docs/src/test/scala/docs/actor/typed/BlockingDispatcherSample.scala) { #blocking-main }

Java
:   @@snip [BlockingDispatcherTest.java](/akka-docs/src/test/java/jdocs/actor/typed/BlockingDispatcherTest.java) { #blocking-main }


Here the app is sending 100 messages to `BlockingActor`s and `PrintActor`s and large numbers
of `akka.actor.default-dispatcher` threads are handling requests. When you run the above code,
you will likely to see the entire application gets stuck somewhere like this:

```
>　PrintActor: 44
>　PrintActor: 45
```

`PrintActor` is considered non-blocking, however it is not able to proceed with handling the remaining messages,
since all the threads are occupied and blocked by the other blocking actors - thus leading to thread starvation.

In the thread state diagrams below the colours have the following meaning:

 * Turquoise - Sleeping state
 * Orange - Waiting state
 * Green - Runnable state

The thread information was recorded using the YourKit profiler, however any good JVM profiler
has this feature (including the free and bundled with the Oracle JDK [VisualVM](https://visualvm.github.io/), as well as [Java Mission Control](https://openjdk.java.net/projects/jmc/)).

The orange portion of the thread shows that it is idle. Idle threads are fine -
they're ready to accept new work. However, a large number of turquoise (blocked, or sleeping as in our example) threads
leads to thread starvation.

@@@ note

[Thread Starvation Detector](https://doc.akka.io/libraries/akka-diagnostics/current/starvation-detector.html)
will issue warning log statements if it detects any of your dispatchers suffering from starvation and other.
It is a helpful first step to identify the problem is occurring in a production system,
and then you can apply the proposed solutions as explained below.

@@@

![dispatcher-behaviour-on-bad-code.png](../images/dispatcher-behaviour-on-bad-code.png)

In the above example we put the code under load by sending hundreds of messages to blocking actors
which causes threads of the default dispatcher to be blocked.
The fork join pool based dispatcher in Akka then attempts to compensate for this blocking by adding more threads to the pool
(`default-akka.actor.default-dispatcher 18,19,20,...`).
This however is not able to help if those too will immediately get blocked,
and eventually the blocking operations will dominate the entire dispatcher.

In essence, the `Thread.sleep` operation has dominated all threads and caused anything
executing on the default dispatcher to starve for resources (including any actor
that you have not configured an explicit dispatcher for).

@@@ div { .group-scala }

### Non-solution: Wrapping in a Future

<!--
  A CompletableFuture by default on ForkJoinPool.commonPool(), so
  because that is already separate from the default dispatcher
  the problem described in these sections do not apply:
-->

When facing this, you
may be tempted to wrap the blocking call inside a `Future` and work
with that instead, but this strategy is too simplistic: you are quite likely to
find bottlenecks or run out of memory or threads when the application runs
under increased load.

Scala
:   @@snip [BlockingDispatcherSample.scala](/akka-docs/src/test/scala/docs/actor/typed/BlockingDispatcherSample.scala) { #blocking-in-future }

The key problematic line here is this:

```scala
implicit val executionContext: ExecutionContext = context.executionContext
```

Using @scala[`context.executionContext`] as the dispatcher on which the blocking `Future`
executes can still be a problem, since this dispatcher is by default used for all other actor processing
unless you @ref:[set up a separate dispatcher for the actor](../dispatchers.md#setting-the-dispatcher-for-an-actor).

@@@

### Non-solution: @scala[`blocking {}`]@java[`ManagedBlocker`]

It may be tempting, if running on a `fork-join-executor`, to signal blocking to the underlying thread pool and allow the
pool to dynamically add a worker thread when about to block.  Constructs like @scala[`Await`]@java[the `get()` and `join()`
methods of `CompletableFuture`] will signal the thread pool in this way.  In the very short term, while the thread which
signaled the pool is blocked, this does keep the dispatcher responsive.  The problem with this approach is that the "spare"
thread will not be stopped until it has been idle for some period of time (the default is 1 minute): in the mean time, this
will likely mean that the spare thread will be running even after the blocked thread has become unblocked.  During this
period, it is further likely that there will be more runnable threads than available processors to run them, which will
generally mean more context switches by the OS kernel and decreased application throughput with unpredictable latencies.
Assuming the system remains under load, these spare threads will be kept busy and not become idle; even a relatively
infrequent use of this mechanism under load may eventually exhaust the ability to add threads, resulting in a crash
"out of the blue".

### Solution: Dedicated dispatcher for blocking operations

An efficient method of isolating the blocking behavior, such that it does not impact the rest of the system,
is to prepare and use a dedicated dispatcher for all those blocking operations.
This technique is often referred to as "bulk-heading" or simply "isolating blocking".

In `application.conf`, the dispatcher dedicated to blocking behavior should
be configured as follows:

<!--same config text for Scala & Java-->
@@snip [BlockingDispatcherSample.scala](/akka-docs/src/test/scala/docs/actor/typed/BlockingDispatcherSample.scala) { #my-blocking-dispatcher-config }

A `thread-pool-executor` based dispatcher allows us to limit the number of threads it will host,
and this way we gain tight control over the maximum number of blocked threads the system may use.

The exact size should be fine tuned depending on the workload you're expecting to run on this dispatcher.

Whenever blocking has to be done, use the above configured dispatcher
instead of the default one:

Scala
:   @@snip [BlockingDispatcherSample.scala](/akka-docs/src/test/scala/docs/actor/typed/BlockingDispatcherSample.scala) { #separate-dispatcher }

Java
:   @@snip [SeparateDispatcherCompletionStageActor.java](/akka-docs/src/test/java/jdocs/actor/typed/SeparateDispatcherCompletionStageActor.java) { #separate-dispatcher }

The thread pool behavior is shown in the below diagram.

![dispatcher-behaviour-on-good-code.png](../images/dispatcher-behaviour-on-good-code.png)

Messages sent to @scala[`SeparateDispatcherFutureActor`]@java[`SeparateDispatcherCompletionStageActor`] and `PrintActor` are handled by the default dispatcher - the
green lines, which represent the actual execution.

When blocking operations are run on the `my-blocking-dispatcher`,
it uses the threads (up to the configured limit) to handle these operations.
The sleeping in this case is nicely isolated to just this dispatcher, and the default one remains unaffected,
allowing the rest of the application to proceed as if nothing bad was happening. After
a certain period of idleness, threads started by this dispatcher will be shut down.

In this case, the throughput of other actors was not impacted -
they were still served on the default dispatcher.

This is the recommended way of dealing with any kind of blocking in reactive
applications.

For a similar discussion specifically about Akka HTTP, refer to @extref[Handling blocking operations in Akka HTTP](akka.http:handling-blocking-operations-in-akka-http-routes.html).

### Solution: Virtual threads dispatcher for blocking operations

If running on Java 21 or later, it is possible to use virtual threads for a blocking dispatcher, configure
the executor of the dispatcher to be `virtual-thread-executor`.

The virtual thread executor will run every task in a virtual thread, which can detach from of the OS-level thread
when it is waiting for a blocking operation, much like how an async task allows threads to be handed back to
a thread pool, until some task completes.

Re-configuring the built-in blocking dispatcher to use virtual threads can be done like this:

```ruby
akka.actor.default-blocking-io-dispatcher {
  executor = "virtual-thread-executor"
}
```

Note that there is a difference in behavior compared to using a thread pool dispatcher in that there is no limit
to how many virtual threads can block, for example hitting a service and waiting for a response, 
while the threadpool executor puts an upper limit (16 by default) on how many threads are actually in flight, 
once that limit has been reached, additional tasks are queued until a thread becomes available.

### Available solutions to blocking operations

The non-exhaustive list of adequate solutions to the “blocking problem”
includes the following suggestions:

 * Do the blocking call within a @scala[`Future`]@java[`CompletionStage`], ensuring an upper bound on
the number of such calls at any point in time (submitting an unbounded
number of tasks of this nature will exhaust your memory or thread limits).
 * Do the blocking call within a `Future`, providing a thread pool with
an upper limit on the number of threads which is appropriate for the
hardware on which the application runs, as explained in detail in this section.
 * Dedicate a single thread to manage a set of blocking resources (e.g. a NIO
selector driving multiple channels) and dispatch events as they occur as
actor messages.
 * Do the blocking call within an actor (or a set of actors) managed by a
@ref:[router](../routing.md), making sure to
configure a thread pool which is either dedicated for this purpose or
sufficiently sized.

The last possibility is especially well-suited for resources which are
single-threaded in nature, like database handles which traditionally can only
execute one outstanding query at a time and use internal synchronization to
ensure this. A common pattern is to create a router for N actors, each of which
wraps a single DB connection and handles queries as sent to the router. The
number N must then be tuned for maximum throughput, which will vary depending
on which DBMS is deployed on what hardware.

@@@ note

Configuring thread pools is a task best delegated to Akka, configure
it in `application.conf` and instantiate through an
@ref:[`ActorSystem`](#dispatcher-lookup)

@@@

## More dispatcher configuration examples

### Fixed pool size

Configuring a dispatcher with fixed thread pool size, e.g. for actors that perform blocking IO:

@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #fixed-pool-size-dispatcher-config }

### Cores

Another example that uses the thread pool based on the number of cores (e.g. for CPU bound tasks)

<!--same config text for Scala & Java-->
@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) {#my-thread-pool-dispatcher-config }

### Pinned

A separate thread is dedicated for each actor that is configured to use the pinned dispatcher.  

Configuring a `PinnedDispatcher`:

<!--same config text for Scala & Java-->
@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) {#my-pinned-dispatcher-config }

Note that `thread-pool-executor` configuration as per the above `my-thread-pool-dispatcher` example is
NOT applicable. This is because every actor will have its own thread pool when using `PinnedDispatcher`,
and that pool will have only one thread.

Note that it's not guaranteed that the *same* thread is used over time, since the core pool timeout
is used for `PinnedDispatcher` to keep resource usage down in case of idle actors. To use the same
thread all the time you need to add `thread-pool-executor.allow-core-timeout=off` to the
configuration of the `PinnedDispatcher`.

### Thread shutdown timeout

Both the `fork-join-executor` and `thread-pool-executor` may shutdown threads when they are not used.
If it's desired to keep the threads alive longer there are some timeout settings that can be adjusted.

<!--same config text for Scala & Java-->
@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) {#my-dispatcher-with-timeouts-config }
 
When using the dispatcher as an `ExecutionContext` without assigning actors to it the `shutdown-timeout` should
typically be increased, since the default of 1 second may cause too frequent shutdown of the entire thread pool.
