# Classic Dispatchers

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[Dispatchers](typed/dispatchers.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Dispatchers are part of core Akka, which means that they are part of the akka-actor dependency:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

<a id="dispatcher-lookup"></a>
## Looking up a Dispatcher

Dispatchers implement the @scala[@scaladoc[ExecutionContext](scala.concurrent.ExecutionContext)]@java[@javadoc[Executor](java.util.concurrent.Executor)] interface and can thus be used to run @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletableFuture](java.util.concurrent.CompletableFuture)] invocations etc.

Scala
:  @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #lookup }

Java
:  @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #lookup }

## Setting the dispatcher for an Actor

So in case you want to give your @apidoc[akka.actor.Actor] a different dispatcher than the default, you need to do two things, of which the first
is to configure the dispatcher:

<!--same config text for Scala & Java-->
@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #my-dispatcher-config }

@@@ note

Note that the `parallelism-max` does not set the upper bound on the total number of threads
allocated by the ForkJoinPool. It is a setting specifically talking about the number of *hot*
threads the pool keep running in order to reduce the latency of handling a new incoming task.
You can read more about parallelism in the JDK's [ForkJoinPool documentation](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.html).

@@@

Another example that uses the "thread-pool-executor":

<!--same config text for Scala & Java-->
@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #fixed-pool-size-dispatcher-config }

@@@ note

The thread pool executor dispatcher is implemented using a @javadoc[java.util.concurrent.ThreadPoolExecutor](java.util.concurrent.ThreadPoolExecutor).
You can read more about it in the JDK's [ThreadPoolExecutor documentation](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ThreadPoolExecutor.html).

@@@

For more options, see @ref[Dispatchers](typed/dispatchers.md) and the `default-dispatcher` section of the @ref:[configuration](general/configuration.md).

Then you create the actor as usual and define the dispatcher in the deployment configuration.

Scala
:  @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #defining-dispatcher-in-config }

Java
:  @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #defining-dispatcher-in-config }

<!--same config text for Scala & Java-->
@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #dispatcher-deployment-config } 

An alternative to the deployment configuration is to define the dispatcher in code.
If you define the `dispatcher` in the deployment configuration then this value will be used instead
of programmatically provided parameter.

Scala
:  @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #defining-dispatcher-in-code }

Java
:  @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #defining-dispatcher-in-code }

@@@ note

The dispatcher you specify in @apidoc[withDispatcher](akka.actor.Props) {scala="#withDispatcher(d:String):akka.actor.Props" java="#withDispatcher(java.lang.String)"} and the `dispatcher` property in the deployment
configuration is in fact a path into your configuration.
So in this example it's a top-level section, but you could for instance put it as a sub-section,
where you'd use periods to denote sub-sections, like this: `"foo.bar.my-dispatcher"`

@@@

