---
project.description: The Akka Actor lifecycle.
---
# Actor lifecycle

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Actors](../actors.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Actor Typed, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-actor-typed_$scala.binary.version$
  version=AkkaVersion
}

## Introduction

An actor is a stateful resource that has to be explicitly started and stopped.

It is important to note that actors do not stop automatically when no longer
referenced, every Actor that is created must also explicitly be destroyed.
The only simplification is that stopping a parent Actor will also recursively
stop all the child Actors that this parent has created. All actors are also
stopped automatically when the @apidoc[ActorSystem](typed.ActorSystem) is shut down.

@@@ note
An `ActorSystem` is a heavyweight structure that will allocate threads,
so create one per logical application. Typically one `ActorSystem` per JVM process.
@@@

## Creating Actors

An actor can create, or _spawn_, an arbitrary number of child actors, which in turn can spawn children of their own, thus
forming an actor hierarchy. @apidoc[akka.actor.typed.ActorSystem] hosts the hierarchy and there can be only one _root actor_,
an actor at the top of the hierarchy of the `ActorSystem`. The lifecycle of a child actor is tied to the parent -- a child
can stop itself or be stopped at any time but it can never outlive its parent.

### The ActorContext

The @apidoc[ActorContext](typed.*.ActorContext) can be accessed for many purposes such as:

* Spawning child actors and supervision
* Watching other actors to receive a @apidoc[Terminated(otherActor)](typed.Terminated) event should the watched actor stop permanently
* Logging
* Creating message adapters
* Request-response interactions (ask) with another actor
* Access to the @scala[@scaladoc[self](akka.actor.typed.scaladsl.ActorContext#self:akka.actor.typed.ActorRef[T])]@java[@javadoc[getSelf()](akka.actor.typed.javadsl.ActorContext#getSelf())] ActorRef

If a behavior needs to use the `ActorContext`, for example to spawn child actors, or use
@scala[`context.self`]@java[`context.getSelf()`], it can be obtained by wrapping construction with @apidoc[Behaviors.setup](typed.*.Behaviors$) {scala="#setup[T](factory:akka.actor.typed.scaladsl.ActorContext[T]=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#setup(akka.japi.function.Function)"}:

Scala
:  @@snip [HelloWorld.scala](/samples/akka-quickstart-scala/src/main/scala/com/example/HelloWorld.scala) { #hello-world-main }

Java
:  @@snip [HelloWorldMain.java](/samples/akka-quickstart-java/src/main/java/com/example/HelloWorldMain.java) { #hello-world-main }

#### ActorContext Thread Safety

Many of the methods in @apidoc[ActorContext](typed.*.ActorContext) are not thread-safe and

* Must not be accessed by threads from @scala[@scaladoc[scala.concurrent.Future](scala.concurrent.Future)]@java[@javadoc[java.util.concurrent.CompletionStage](java.util.concurrent.CompletionStage)] callbacks
* Must not be shared between several actor instances
* Must only be used in the ordinary actor message processing thread

### The Guardian Actor

The top level actor, also called the user guardian actor, is created along with the @apidoc[ActorSystem](typed.ActorSystem). Messages sent to the actor
system are directed to the root actor. The root actor is defined by the behavior used to create the `ActorSystem`,
named `HelloWorldMain` in the example below:

Scala
:  @@snip [HelloWorld.scala](/samples/akka-quickstart-scala/src/main/scala/com/example/HelloWorld.scala) { #hello-world }

Java
:  @@snip [HelloWorldMain.java](/samples/akka-quickstart-java/src/main/java/com/example/HelloWorldMain.java) { #hello-world }

For very simple applications the guardian may contain the actual application logic and handle messages. As soon as the application
handles more than one concern the guardian should instead just bootstrap the application, spawn the various subsystems as
children and monitor their lifecycles.

When the guardian actor stops this will stop the `ActorSystem`.

When @apidoc[ActorSystem.terminate](typed.ActorSystem) {scala="#terminate():Unit" java="#terminate()"} is invoked the @ref:[Coordinated Shutdown](../coordinated-shutdown.md) process will
stop actors and services in a specific order.

@@@ Note

In the classic counter part, the @apidoc[akka.actor.ActorSystem], the root actor was provided out of the box and you
could spawn top-level actors from the outside of the `ActorSystem` using `actorOf`. @ref:[SpawnProtocol](#spawnprotocol)
is a tool that mimics the old style of starting up actors.

@@@


### Spawning Children

Child actors are created and started with `ActorContext`'s @apidoc[spawn](typed.*.ActorContext) {scala="#spawn[U](behavior:akka.actor.typed.Behavior[U],name:String,props:akka.actor.typed.Props):akka.actor.typed.ActorRef[U]" java="#spawn(akka.actor.typed.Behavior,java.lang.String)"}.
In the example below, when the root actor
is started, it spawns a child actor described by the `HelloWorld` behavior. Additionally, when the root actor receives a
`SayHello` message, it creates a child actor defined by the behavior `HelloWorldBot`:

Scala
:  @@snip [HelloWorld.scala](/samples/akka-quickstart-scala/src/main/scala/com/example/HelloWorld.scala) { #hello-world-main }

Java
:  @@snip [HelloWorldMain.java](/samples/akka-quickstart-java/src/main/java/com/example/HelloWorldMain.java) { #hello-world-main }

To specify a dispatcher when spawning an actor use @apidoc[DispatcherSelector]. If not specified, the actor will
use the default dispatcher, see @ref:[Default dispatcher](dispatchers.md#default-dispatcher) for details.

Scala
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/IntroSpec.scala) { #hello-world-main-with-dispatchers }

Java
:  @@snip [IntroTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/IntroTest.java) { #hello-world-main-with-dispatchers }

Refer to @ref:[Actors](actors.md#first-example) for a walk-through of the above examples.

### SpawnProtocol

The guardian actor should be responsible for initialization of tasks and create the initial actors of the application,
but sometimes you might want to spawn new actors from the outside of the guardian actor. For example creating one actor
per HTTP request.

That is not difficult to implement in your behavior, but since this is a common pattern there is a predefined
message protocol and implementation of a behavior for this. It can be used as the guardian actor of the @apidoc[ActorSystem](typed.ActorSystem),
possibly combined with @apidoc[Behaviors.setup](typed.*.Behaviors$) {scala="#setup[T](factory:akka.actor.typed.scaladsl.ActorContext[T]=%3Eakka.actor.typed.Behavior[T]):akka.actor.typed.Behavior[T]" java="#setup(akka.japi.function.Function)"} to start some initial tasks or actors. Child actors can then be started from
the outside by @apidoc[tell](typed.ActorRef) {scala="#tell(msg:T):Unit" java="#tell(T)"}ing or @scala[@scaladoc[ask](akka.actor.typed.scaladsl.AskPattern.Askable#ask[Res](replyTo:akka.actor.typed.ActorRef[Res]=%3EReq)(implicittimeout:akka.util.Timeout,implicitscheduler:akka.actor.typed.Scheduler):scala.concurrent.Future[Res])]@java[@javadoc[ask](akka.actor.typed.javadsl.AskPattern#ask(akka.actor.typed.RecipientRef,akka.japi.function.Function,java.time.Duration,akka.actor.typed.Scheduler))]ing @apidoc[SpawnProtocol.Spawn] to the actor reference of the system. Using `ask` is
similar to how `ActorSystem.actorOf` can be used in classic actors with the difference that a
@scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] of the @apidoc[ActorRef](typed.ActorRef) is returned.

The guardian behavior can be defined as:

Scala
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/SpawnProtocolDocSpec.scala) { #imports1 #main }

Java
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/SpawnProtocolDocTest.java) { #imports1 #main }

and the @apidoc[ActorSystem](typed.ActorSystem) can be created with that `main` behavior and asked to spawn other actors:

Scala
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/SpawnProtocolDocSpec.scala) { #imports2 #system-spawn }

Java
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/SpawnProtocolDocTest.java) { #imports2 #system-spawn }

The @apidoc[SpawnProtocol$] can also be used at other places in the actor hierarchy. It doesn't have to be the root
guardian actor.

A way to find running actors is described in @ref:[Actor discovery](actor-discovery.md).

## Stopping Actors

An actor can stop itself by returning @apidoc[Behaviors.stopped](typed.*.Behaviors$) {scala="#stopped[T]:akka.actor.typed.Behavior[T]" java="#stopped()"} as the next behavior.

A child actor can be forced to stop after it finishes processing its current message by using the
@apidoc[stop](typed.*.ActorContext) {scala="#stop[U](child:akka.actor.typed.ActorRef[U]):Unit" java="#stop(akka.actor.typed.ActorRef)"} method of the `ActorContext` from the parent actor. Only child actors can be stopped in that way.

All child actors will be stopped when their parent is stopped.

When an actor is stopped, it receives the @apidoc[PostStop](typed.PostStop) signal that can be used for cleaning up resources.

Here is an illustrating example:

Scala
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/GracefulStopDocSpec.scala) {
    #imports
    #master-actor
    #worker-actor
  }

Java
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/GracefulStopDocTest.java)  {
   #imports
   #master-actor
   #worker-actor
 }

When cleaning up resources from `PostStop` you should also consider doing the same for the @apidoc[PreRestart](typed.PreRestart) signal,
which is emitted when the @ref:[actor is restarted](fault-tolerance.md#the-prerestart-signal). Note that `PostStop`
is not emitted for a restart. 

## Watching Actors

In order to be notified when another actor terminates (i.e. stops permanently, not temporary failure and restart),
an actor can @apidoc[watch](typed.*.ActorContext) {scala="#watch[U](other:akka.actor.typed.ActorRef[U]):Unit" java="#watch(akka.actor.typed.ActorRef)"} another actor. It will receive the @apidoc[akka.actor.typed.Terminated] signal upon
termination (see @ref:[Stopping Actors](#stopping-actors)) of the watched actor.

Scala
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/GracefulStopDocSpec.scala) { #master-actor-watch }

Java
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/GracefulStopDocTest.java)  { #master-actor-watch }

An alternative to @apidoc[watch](typed.*.ActorContext) {scala="#watch[U](other:akka.actor.typed.ActorRef[U]):Unit" java="#watch(akka.actor.typed.ActorRef)"} is @apidoc[watchWith](typed.*.ActorContext) {scala="#watchWith[U](other:akka.actor.typed.ActorRef[U],msg:T):Unit" java="#watchWith(akka.actor.typed.ActorRef,T)"}, which allows specifying a custom message instead of the `Terminated`.
This is often preferred over using `watch` and the `Terminated` signal because additional information can
be included in the message that can be used later when receiving it.

Similar example as above, but using `watchWith` and replies to the original requestor when the job has finished.

Scala
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/GracefulStopDocSpec.scala) { #master-actor-watchWith }

Java
:  @@snip [IntroSpec.scala](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/GracefulStopDocTest.java)  { #master-actor-watchWith }

Note how the `replyToWhenDone` is included in the `watchWith` message and then used later when receiving the
`JobTerminated` message. 

The watched actor can be any @apidoc[ActorRef](typed.ActorRef), it doesn't have to be a child actor as in the above example.

It should be noted that the terminated message is generated independent of the order in which registration
and termination occur. In particular, the watching actor will receive a terminated message even if the
watched actor has already been terminated at the time of registration.

Registering multiple times does not necessarily lead to multiple messages being generated, but there is no
guarantee that only exactly one such message is received: if termination of the watched actor has generated and queued
the message, and another registration is done before this message has been processed, then a second message will be
queued, because registering for monitoring of an already terminated actor leads to the immediate generation of
the terminated message.

It is also possible to deregister from watching another actor’s liveliness using @apidoc[context.unwatch(target)](typed.*.ActorContext) {scala="#unwatch[U](other:akka.actor.typed.ActorRef[U]):Unit" java="#unwatch(akka.actor.typed.ActorRef)"}.
This works even if the terminated message has already been enqueued in the mailbox; after calling `unwatch`
no terminated message for that actor will be processed anymore.

The terminated message is also sent when the watched actor is on a node that has been removed from the
@ref:[Cluster](cluster.md).
