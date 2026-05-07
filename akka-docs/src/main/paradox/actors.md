# Classic Actors

@@include[includes.md](includes.md) { #actor-api }

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Classic Actors, add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
  group2="com.typesafe.akka"
  artifact2="akka-testkit_$scala.binary.version$"
  scope2=test
  version2=AkkaVersion
}

@@project-info{ projectId="akka-actor" }

## Introduction

The [Actor Model](https://en.wikipedia.org/wiki/Actor_model) provides a higher level of abstraction for writing concurrent
and distributed systems. It alleviates the developer from having to deal with
explicit locking and thread management, making it easier to write correct
concurrent and parallel systems. Actors were defined in the 1973 paper by Carl
Hewitt but have been popularized by the Erlang language and used for example at
Ericsson with great success to build highly concurrent and reliable telecom
systems.

The API of Akka’s Actors is similar to Scala Actors which has borrowed some of
its syntax from Erlang.

## Creating Actors

@@@ note

Since Akka enforces parental supervision every actor is supervised and
(potentially) the supervisor of its children, it is advisable to
familiarize yourself with @ref:[Actor Systems](general/actor-systems.md), @ref:[supervision](general/supervision.md)
and @ref:[handling exceptions](general/supervision.md#actors-and-exceptions)
as well as @ref:[Actor References, Paths and Addresses](general/addressing.md).

@@@

### Defining an Actor class

@@@ div { .group-scala }

Actors are implemented by extending the @scaladoc[Actor](akka.actor.Actor) base trait and implementing the
@scaladoc[receive](akka.actor.Actor#receive:akka.actor.Actor.Receive) method. The `receive` method should define a series of case
statements (which has the type `PartialFunction[Any, Unit]`) that define
which messages your Actor can handle, using standard Scala pattern matching,
along with the implementation of how the messages should be processed.

@@@

@@@ div { .group-java }

Actor classes are implemented by extending the @javadoc[AbstractActor](akka.actor.AbstractActor) class and setting
the “initial behavior” in the @javadoc[createReceive](akka.actor.AbstractActor#createReceive()) method.

The `createReceive` method has no arguments and returns @javadoc[AbstractActor.Receive](akka.actor.AbstractActor.Receive). It defines which messages your Actor can handle, along with the implementation of how the messages should be processed. You can build such behavior with a builder named
@javadoc[ReceiveBuilder](akka.actor.typed.javadsl.ReceiveBuilder). This build has a convenient factory in `AbstractActor` called @javadoc[receiveBuilder](akka.actor.AbstractActor#receiveBuilder()).

@@@

Here is an example:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #imports1 #my-actor }

Java
:  @@snip [MyActor.java](/akka-docs/src/test/java/jdocs/actor/MyActor.java) { #imports #my-actor }

Please note that the Akka Actor @scala[`receive`] message loop is exhaustive, which is different compared to Erlang and the late Scala Actors. This means that you
need to provide a pattern match for all messages that it can accept and if you
want to be able to handle unknown messages then you need to have a default case
as in the example above. Otherwise an @apidoc[akka.actor.UnhandledMessage] will be published to the @apidoc[akka.actor.ActorSystem]'s
@apidoc[akka.event.EventStream].

Note further that the return type of the behavior defined above is @scaladoc[Unit](scala.Unit); if
the actor shall reply to the received message then this must be done explicitly
as explained below.

The result of the @scala[@scaladoc[receive](akka.actor.Actor#receive:akka.actor.Actor.Receive) method is a partial function object, which is]
@java[@javadoc[createReceive](akka.actor.AbstractActor#createReceive()) method is @javadoc[AbstractActor.Receive](akka.actor.AbstractActor.Receive) which is a wrapper around partial
scala function object. It is] stored within the actor as its “initial behavior”,
see @ref:[Become/Unbecome](#become-unbecome) for
further information on changing the behavior of an actor after its
construction.

### Props

@apidoc[akka.actor.Props] is a configuration class to specify options for the creation
of actors, think of it as an immutable and thus freely shareable recipe for
creating an actor including associated deployment information (e.g., which
dispatcher to use, see more below). Here are some examples of how to create a
`Props` instance.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #creating-props }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #import-props #creating-props }


The second variant shows how to pass constructor arguments to the
@apidoc[akka.actor.Actor] being created, but it should only be used outside of actors as
explained below.

The last line shows a possibility to pass constructor arguments regardless of
the context it is being used in. The presence of a matching constructor is
verified during construction of the `Props` object, resulting in an
@javadoc[IllegalArgumentException](java.lang.IllegalArgumentException) if no or multiple matching constructors are
found.

@@@ note { .group-scala }

The recommended approach to create the actor @apidoc[akka.actor.Props] is not supported
for cases when the actor constructor takes value classes as arguments.

@@@

#### Dangerous Variants
Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #creating-props-deprecated }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #creating-props-deprecated }

This method is not recommended being used within another actor because it
encourages to close over the enclosing scope, resulting in non-serializable
`Props` and possibly race conditions (breaking the actor encapsulation).
On the other hand, using this variant in a `Props` factory in
the actor’s companion object as documented under “Recommended Practices” below
is completely fine.

There were two use-cases for these methods: passing constructor arguments to
the actor—which is solved by the newly introduced
@scala[@scaladoc[Props.apply(clazz, args)](akka.actor.Props$#apply(clazz:Class[_],args:Any*):akka.actor.Props)] @java[@javadoc[Props.create(clazz, args)](akka.actor.Props#create(java.lang.Class,java.lang.Object...))] method above or the recommended practice
below—and creating actors “on the spot” as anonymous classes. The latter should
be solved by making these actors named classes instead (if they are not
declared within a top-level `object` then the enclosing instance’s `this`
reference needs to be passed as the first argument).

@@@ warning

Declaring one actor within another is very dangerous and breaks actor
encapsulation. Never pass an actor’s `this` reference into `Props`!

@@@

@@@ div { .group-scala }

#### Edge cases

There are two edge cases in actor creation with @scaladoc[akka.actor.Props](akka.actor.Props):

* An actor with @scaladoc[AnyVal](scala.AnyVal) arguments.

@@snip [PropsEdgeCaseSpec.scala](/akka-docs/src/test/scala/docs/actor/PropsEdgeCaseSpec.scala) { #props-edge-cases-value-class }

@@snip [PropsEdgeCaseSpec.scala](/akka-docs/src/test/scala/docs/actor/PropsEdgeCaseSpec.scala) { #props-edge-cases-value-class-example }

* An actor with default constructor values.

@@snip [PropsEdgeCaseSpec.scala](/akka-docs/src/test/scala/docs/actor/PropsEdgeCaseSpec.scala) { #props-edge-cases-default-values }

In both cases, an @javadoc[IllegalArgumentException](java.lang.IllegalArgumentException) will be thrown stating
no matching constructor could be found.

The next section explains the recommended ways to create @scaladoc[Actor](akka.actor.Actor) props in a way,
which simultaneously safe-guards against these edge cases.

@@@

#### Recommended Practices

It is a good idea to provide @scala[factory methods on the companion object of each
@apidoc[Actor](akka.actor.Actor)] @java[static factory methods for each @apidoc[Actor](akka.actor.Actor)] which help keeping the creation of
suitable `Props` as close to the actor definition as possible. This also avoids the pitfalls
associated with using the @scala[@scaladoc[Props.apply(...)](akka.actor.Props$#apply(clazz:Class[_],args:Any*):akka.actor.Props) method which takes a by-name
argument, since within a companion object] @java[ @javadoc[Props.create(...)](akka.actor.Props#create(java.lang.Class,java.lang.Object...)) method which takes
arguments as constructor parameters, since within static method]
the given code block will not retain a reference to its enclosing scope:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #props-factory }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #props-factory }

Another good practice is to declare what messages an Actor can receive
@scala[in the companion object of the Actor]
@java[as close to the actor definition as possible (e.g. as static classes inside the Actor or using other suitable class)],
which makes easier to know what it can receive:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #messages-in-companion }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #messages-in-companion }

### Creating Actors with Props

Actors are created by passing a @apidoc[akka.actor.Props] instance into the
`actorOf` factory method which is available on @apidoc[akka.actor.ActorSystem] and
@apidoc[akka.actor.ActorContext].

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #system-actorOf }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #import-actorRef }

Using the `ActorSystem` will create top-level actors, supervised by the
actor system’s provided guardian actor while using an actor’s context will
create a child actor.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #context-actorOf }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #context-actorOf }

It is recommended to create a hierarchy of children, grand-children and so on
such that it fits the logical failure-handling structure of the application,
see @ref:[Actor Systems](general/actor-systems.md).

The call to `actorOf` returns an instance of @apidoc[akka.actor.ActorRef]. This is a
handle to the actor instance and the only way to interact with it. The
`ActorRef` is immutable and has a one to one relationship with the Actor
it represents. The `ActorRef` is also serializable and network-aware.
This means that you can serialize it, send it over the wire and use it on a
remote host, and it will still be representing the same Actor on the original
node, across the network.

The name parameter is optional, but you should preferably name your actors,
since that is used in log messages and for identifying actors. The name must
not be empty or start with `$`, but it may contain URL encoded characters
(eg., `%20` for a blank space).  If the given name is already in use by
another child to the same parent an @apidoc[akka.actor.InvalidActorNameException] is thrown.

Actors are automatically started asynchronously when created.

@@@ div { .group-scala }

#### Value classes as constructor arguments

The recommended way to instantiate actor props uses reflection at runtime
to determine the correct actor constructor to be invoked and due to technical
limitations it is not supported when said constructor takes arguments that are
value classes.
In these cases you should either unpack the arguments or create the props by
calling the constructor manually:

@@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #actor-with-value-class-argument }

@@@

### Dependency Injection

If your @apidoc[akka.actor.Actor] has a constructor that takes parameters then those need to
be part of the @apidoc[akka.actor.Props] as well, as described @ref:[above](#props). But there
are cases when a factory method must be used, for example when the actual
constructor arguments are determined by a dependency injection framework.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #creating-indirectly }

Java
:  @@snip [DependencyInjectionDocTest.java](/akka-docs/src/test/java/jdocs/actor/DependencyInjectionDocTest.java) { #import #creating-indirectly }

@@@ warning

You might be tempted at times to offer an @apidoc[akka.actor.IndirectActorProducer]
which always returns the same instance, e.g. by using a @scala[`lazy val`.] @java[static field.] This is
not supported, as it goes against the meaning of an actor restart, which is
described here: @ref:[What Restarting Means](general/supervision.md#supervision-restart).

When using a dependency injection framework, actor beans *MUST NOT* have
singleton scope.

@@@

Techniques for dependency injection and integration with dependency injection frameworks
are described in more depth in the
[Using Akka with Dependency Injection](https://letitcrash.com/post/55958814293/akka-dependency-injection)
guideline and the [Akka Java Spring](https://github.com/typesafehub/activator-akka-java-spring) tutorial.

## Actor API

@scala[The @apidoc[akka.actor.Actor] trait defines only one abstract method, the above mentioned
`receive`, which implements the behavior of the actor.]
@java[The @apidoc[akka.actor.AbstractActor] class defines a method called `createReceive`,
that is used to set the “initial behavior” of the actor.]

If the current actor behavior does not match a received message,
`unhandled` is called, which by default publishes an
@apidoc[akka.actor.UnhandledMessage(message, sender, recipient)](akka.actor.UnhandledMessage) on the actor
system’s event stream (set configuration item
`akka.actor.debug.unhandled` to `on` to have them converted into
actual Debug messages).

In addition, it offers:

* @scala[@scaladoc[self](akka.actor.Actor#self:akka.actor.ActorRef)] @java[@javadoc[getSelf()](akka.actor.AbstractActor#getSelf())] reference to the @apidoc[akka.actor.ActorRef] of the actor
* @scala[@scaladoc[sender](akka.actor.Actor#sender():akka.actor.ActorRef)] @java[@javadoc[getSender()](akka.actor.AbstractActor#getSender())] reference sender Actor of the last received message, typically used as described in
  @scala[[Actor.Reply](#actor-reply)]
  @java[[LambdaActor.Reply](#lambdaactor-reply)]
* @scala[@scaladoc[supervisorStrategy](akka.actor.Actor#supervisorStrategy:akka.actor.SupervisorStrategy)] @java[@javadoc[supervisorStrategy()](akka.actor.AbstractActor#supervisorStrategy())] user overridable definition the strategy to use for supervising child actors

  This strategy is typically declared inside the actor to have access
  to the actor’s internal state within the decider function: since failure is
  communicated as a message sent to the supervisor and processed like other
  messages (albeit outside the normal behavior), all values and variables
  within the actor are available, as is the `sender` reference (which will
  be the immediate child reporting the failure; if the original failure
  occurred within a distant descendant it is still reported one level up at a
  time).

* @scala[@scaladoc[context](akka.actor.Actor#context:akka.actor.ActorContext)] @java[@javadoc[getContext()](akka.actor.AbstractActor#getContext())] exposes contextual information for the actor and the current message, such as:
    * factory methods to create child actors (`actorOf`)
    * system that the actor belongs to
    * parent supervisor
    * supervised children
    * lifecycle monitoring
    * hotswap behavior stack as described in @scala[[Actor.HotSwap](#actor-hotswap)] @java[[Become/Unbecome](#actor-hotswap)]

@@@ div { .group-scala }

You can import the members in the `context` to avoid prefixing access with `context.`

@@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #import-context }

@@@

The remaining visible methods are user-overridable life-cycle hooks which are
described in the following:

Scala
:  @@snip [Actor.scala](/akka-actor/src/main/scala/akka/actor/Actor.scala) { #lifecycle-hooks }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #lifecycle-callbacks }

The implementations shown above are the defaults provided by the @scala[@scaladoc[Actor](akka.actor.Actor) trait.] @java[@javadoc[AbstractActor](akka.actor.AbstractActor) class.]

### Actor Lifecycle

![actor_lifecycle.png](./images/actor_lifecycle.png)

A path in an actor system represents a "place" that might be occupied
by a living actor. Initially (apart from system initialized actors) a path is
empty. When `actorOf()` is called it assigns an *incarnation* of the actor
described by the passed @apidoc[akka.actor.Props] to the given path. An actor incarnation is
identified by the path *and a UID*.

It is worth noting about the difference between:

* restart
* stop, followed by a re-creation of the actor

as explained below.

A restart only swaps the @apidoc[akka.actor.Actor]
instance defined by the @apidoc[akka.actor.Props] but the incarnation and hence the UID remains
the same.
As long as the incarnation is the same, you can keep using the same @apidoc[akka.actor.ActorRef].
Restart is handled by the @ref:[Supervision Strategy](fault-tolerance.md#creating-a-supervisor-strategy) of actor's parent actor,
and there is more discussion about @ref:[what restart means](general/supervision.md#supervision-restart).

The lifecycle of an incarnation ends when the actor is stopped. At
that point, the appropriate lifecycle events are called and watching actors
are notified of the termination. After the incarnation is stopped, the path can
be reused again by creating an actor with `actorOf()`. In this case, the
name of the new incarnation will be the same as the previous one, but the
UIDs will differ. An actor can be stopped by the actor itself, another actor
or the @apidoc[ActorSystem](akka.actor.ActorSystem) (see @ref:[Stopping actors](#stopping-actors)).

@@@ note

It is important to note that Actors do not stop automatically when no longer
referenced, every Actor that is created must also explicitly be destroyed.
The only simplification is that stopping a parent Actor will also recursively
stop all the child Actors that this parent has created.

@@@

An @apidoc[akka.actor.ActorRef] always represents an incarnation (path and UID) not just a
given path. Therefore, if an actor is stopped, and a new one with the same
name is created then an `ActorRef` of the old incarnation will not point
to the new one.

@apidoc[akka.actor.ActorSelection] on the other hand points to the path (or multiple paths
if wildcards are used) and is completely oblivious to which incarnation is currently
occupying it. `ActorSelection` cannot be watched for this reason. It is
possible to resolve the current incarnation's `ActorRef` living under the
path by sending an @apidoc[akka.actor.Identify] message to the `ActorSelection` which
will be replied to with an @apidoc[akka.actor.ActorIdentity] containing the correct reference
(see @ref:[ActorSelection](#actorselection)). This can also be done with the @apidoc[resolveOne](akka.actor.ActorSelection) {scala="#resolveOne(timeout:scala.concurrent.duration.FiniteDuration):scala.concurrent.Future[akka.actor.ActorRef]" java="#resolveOne(java.time.Duration)"}
method of the `ActorSelection`, which returns a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] of the matching
`ActorRef`.

<a id="deathwatch"></a>
### Lifecycle Monitoring aka DeathWatch

To be notified when another actor terminates (i.e., stops permanently,
not a temporary failure and restart), an actor may register itself for reception
of the @apidoc[akka.actor.Terminated] message dispatched by the other actor upon
termination (see @ref:[Stopping Actors](#stopping-actors)). This service is provided by the
`DeathWatch` component of the actor system.

Registering a monitor is easy:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #watch }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #import-terminated #watch }

It should be noted that the @apidoc[akka.actor.Terminated] message is generated
independently of the order in which registration and termination occur.
In particular, the watching actor will receive a `Terminated` message even if the
watched actor has already been terminated at the time of registration.

Registering multiple times does not necessarily lead to multiple messages being
generated, but there is no guarantee that only exactly one such message is
received: if termination of the watched actor has generated and queued the
message, and another registration is done before this message has been
processed, then a second message will be queued because registering for
monitoring of an already terminated actor leads to the immediate generation of
the `Terminated` message.

It is also possible to deregister from watching another actor’s liveliness
using @apidoc[context.unwatch(target)](akka.actor.ActorContext) {scala="#unwatch(subject:akka.actor.ActorRef):akka.actor.ActorRef" java="#unwatch(akka.actor.ActorRef)"}. This works even if the `Terminated`
message has already been enqueued in the mailbox; after calling `unwatch`
no `Terminated` message for that actor will be processed anymore.

### Start Hook

Right after starting the actor, its @scala[@scaladoc[preStart](akka.actor.Actor#preStart():Unit)]@java[@javadoc[preStart](akka.actor.AbstractActor#preStart())] method is invoked.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #preStart }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #preStart }

This method is called when the actor is first created. During restarts, it is
called by the default implementation of @scala[@scaladoc[postRestart](akka.actor.Actor#postRestart(reason:Throwable):Unit)]@java[@javadoc[postRestart](akka.actor.AbstractActor#postRestart(java.lang.Throwable))], which means that
by overriding that method you can choose whether the initialization code in
this method is called only exactly once for this actor or every restart.
Initialization code which is part of the actor’s constructor will always be
called when an instance of the actor class is created, which happens at every
restart.

<a id="restart-hook"></a>
### Restart Hooks

All actors are supervised, i.e., linked to another actor with a fault
handling strategy. Actors may be restarted in case an exception is thrown while
processing a message (see @ref:[supervision](general/supervision.md)). This restart involves the hooks
mentioned above:

1. The old actor is informed by calling @scala[@scaladoc[preRestart](akka.actor.Actor#preRestart(reason:Throwable,message:Option[Any]):Unit)]@java[@javadoc[preRestart](akka.actor.AbstractActor#preRestart(java.lang.Throwable,java.util.Optional))] with the exception
   which caused the restart, and the message which triggered that exception; the
   latter may be `None` if the restart was not caused by processing a
   message, e.g. when a supervisor does not trap the exception and is restarted
   in turn by its supervisor, or if an actor is restarted due to a sibling’s
   failure. If the message is available, then that message’s sender is also
   accessible in the usual way (i.e. by calling @scala[@scaladoc[sender](akka.actor.Actor#sender():akka.actor.ActorRef)] @java[@javadoc[getSender()](akka.actor.AbstractActor#getSender())]).
   This method is the best place for cleaning up, preparing hand-over to the
   fresh actor instance, etc.  By default, it stops all children and calls
   @scala[@scaladoc[postStop](akka.actor.Actor#postStop():Unit)]@java[@javadoc[postStop](akka.actor.AbstractActor#postStop())].
2. The initial factory from the `actorOf` call is used
   to produce the fresh instance.
3. The new actor’s `postRestart` method is invoked with the exception
   which caused the restart. By default the `preStart`
   is called, just as in the normal start-up case.

An actor restart replaces only the actual actor object; the contents of the
mailbox are unaffected by the restart, so the processing of messages will resume
after the `postRestart` hook returns. The message
that triggered the exception will not be received again. Any message
sent to an actor while it is being restarted will be queued to its mailbox as
usual.

@@@ warning

Be aware that the ordering of failure notifications relative to user messages
is not deterministic. In particular, a parent might restart its child before
it has processed the last messages sent by the child before the failure.
See @ref:[Discussion: Message Ordering](general/message-delivery-reliability.md#message-ordering) for details.

@@@

### Stop Hook

After stopping an actor, its @scala[@scaladoc[postStop](akka.actor.Actor#postStop():Unit)]@java[@javadoc[postStop](akka.actor.AbstractActor#postStop())] hook is called, which may be used
e.g. for deregistering this actor from other services. This hook is guaranteed
to run after message queuing has been disabled for this actor, i.e. messages
sent to a stopped actor will be redirected to the @apidoc[deadLetters](akka.actor.ActorSystem) {scala="#deadLetters:akka.actor.ActorRef" java="#deadLetters()"} of the
@apidoc[akka.actor.ActorSystem].

<a id="actorselection"></a>
## Identifying Actors via Actor Selection

As described in @ref:[Actor References, Paths and Addresses](general/addressing.md), each actor has a unique logical path, which
is obtained by following the chain of actors from child to parent until
reaching the root of the actor system, and it has a physical path, which may
differ if the supervision chain includes any remote supervisors. These paths
are used by the system to look up actors, e.g. when a remote message is
received and the recipient is searched, but they are also useful more directly:
actors may look up other actors by specifying absolute or relative
paths—logical or physical—and receive back an @apidoc[akka.actor.ActorSelection] with the
result:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #selection-local }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #selection-local }

@@@ note

It is always preferable to communicate with other Actors using their ActorRef
instead of relying upon ActorSelection. Exceptions are

* sending messages using the @ref:[At-Least-Once Delivery](persistence.md#at-least-once-delivery) facility
* initiating the first contact with a remote system

In all other cases, ActorRefs can be provided during Actor creation or
initialization, passing them from parent to child or introducing Actors by
sending their ActorRefs to other Actors within messages.

@@@

The supplied path is parsed as a @javadoc[java.net.URI](java.net.URI), which means
that it is split on `/` into path elements. If the path starts with `/`, it
is absolute and the look-up starts at the root guardian (which is the parent of
`"/user"`); otherwise it starts at the current actor. If a path element equals
`..`, the look-up will take a step “up” towards the supervisor of the
currently traversed actor, otherwise it will step “down” to the named child.
It should be noted that the `..` in actor paths here always means the logical
structure, i.e. the supervisor.

The path elements of an actor selection may contain wildcard patterns allowing for
broadcasting of messages to that section:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #selection-wildcard }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #selection-wildcard }

Messages can be sent via the @apidoc[akka.actor.ActorSelection] and the path of the
`ActorSelection` is looked up when delivering each message. If the selection
does not match any actors the message will be dropped.

To acquire an @apidoc[akka.actor.ActorRef] for an `ActorSelection` you need to send
a message to the selection and use the @scala[@scaladoc[sender](akka.actor.Actor#sender():akka.actor.ActorRef)] @java[@javadoc[getSender()](akka.actor.AbstractActor#getSender())]) reference of the reply from
the actor. There is a built-in @apidoc[akka.actor.Identify] message that all Actors will
understand and automatically reply to with an @apidoc[akka.actor.ActorIdentity] message
containing the @apidoc[akka.actor.ActorRef]. This message is handled specially by the
actors which are traversed in the sense that if a concrete name lookup fails
(i.e. a non-wildcard path element does not correspond to a live actor) then a
negative result is generated. Please note that this does not mean that delivery
of that reply is guaranteed, it still is a normal message.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #identify }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #import-identify #identify }

You can also acquire an `ActorRef` for an `ActorSelection` with
the @apidoc[resolveOne](akka.actor.ActorSelection) {scala="#resolveOne(timeout:scala.concurrent.duration.FiniteDuration):scala.concurrent.Future[akka.actor.ActorRef]" java="#resolveOne(java.time.Duration)"} method of the `ActorSelection`. It returns a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)]
of the matching `ActorRef` if such an actor exists. It is completed with
failure @apidoc[akka.actor.ActorNotFound] if no such actor exists or the identification
didn't complete within the supplied `timeout`.

Remote actor addresses may also be looked up, if @ref:[remoting](remoting-artery.md) is enabled:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #selection-remote }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #selection-remote }

An example demonstrating actor look-up is given in @ref:[Remoting Sample](remoting-artery.md#looking-up-remote-actors).

## Messages and immutability

@@@ warning { title=IMPORTANT }

Messages can be any kind of object but have to be immutable. @scala[Scala] @java[Akka] can’t enforce
immutability (yet) so this has to be by convention. @scala[Primitives like String, Int,
Boolean are always immutable. Apart from these the recommended approach is to
use Scala case classes that are immutable (if you don’t explicitly expose the
state) and works great with pattern matching at the receiver side.]

@@@

Here is an @scala[example:] @java[example of an immutable message:]

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #immutable-message-definition #immutable-message-instantiation }

Java
:  @@snip [ImmutableMessage.java](/akka-docs/src/test/java/jdocs/actor/ImmutableMessage.java) { #immutable-message }


## Send messages

Messages are sent to an Actor through one of the following methods.

* @scala[@scaladoc[!](akka.actor.ActorRef#!(message:Any)(implicitsender:akka.actor.ActorRef):Unit)] @java[@javadoc[tell](akka.actor.ActorRef#tell(java.lang.Object,akka.actor.ActorRef))] means “fire-and-forget”, e.g. send a message asynchronously and return
  immediately. @scala[Also known as `tell`.]
* @scala[`?`] @java[`ask`] sends a message asynchronously and returns a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)]
  representing a possible reply. @scala[Also known as `ask`.]

Message ordering is guaranteed on a per-sender basis.

@@@ note

There are performance implications of using `ask` since something needs to
keep track of when it times out, there needs to be something that bridges
a `Promise` into an @apidoc[akka.actor.ActorRef] and it also needs to be reachable through
remoting. So always prefer `tell` for performance, and only `ask` if you must.

@@@

@@@ div { .group-java }

In all these methods you have the option of passing along your own @apidoc[akka.actor.ActorRef].
Make it a practice of doing so because it will allow the receiver actors to be able to respond
to your message since the sender reference is sent along with the message.

@@@

<a id="actors-tell-sender"></a>
### Tell: Fire-forget

This is the preferred way of sending messages. No blocking waiting for a
message. This gives the best concurrency and scalability characteristics.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #tell }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #tell }

@@@ div { .group-scala }

If invoked from within an Actor, then the sending actor reference will be
implicitly passed along with the message and available to the receiving Actor
in its `sender(): ActorRef` member method. The target actor can use this
to reply to the original sender, by using `sender() ! replyMsg`.

If invoked from an instance that is **not** an Actor the sender will be
@scaladoc[deadLetters](akka.actor.ActorSystem#deadLetters:akka.actor.ActorRef) actor reference by default.

@@@

@@@ div { .group-java }

The sender reference is passed along with the message and available within the
receiving actor via its @javadoc[getSender()](akka.actor.AbstractActor#getSender()) method while processing this
message. Inside of an actor it is usually @javadoc[getSelf()](akka.actor.AbstractActor#getSelf()) who shall be the
sender, but there can be cases where replies shall be routed to some other
actor—e.g. the parent—in which the second argument to @javadoc[tell](akka.actor.ActorRef#tell(java.lang.Object,akka.actor.ActorRef)) would be a
different one. Outside of an actor and if no reply is needed the second
argument can be `null`; if a reply is needed outside of an actor you can use
the ask-pattern described next.

@@@

<a id="actors-ask"></a>
### Ask: Send-And-Receive-Future

The `ask` pattern involves actors as well as futures, hence it is offered as
a use pattern rather than a method on @apidoc[akka.actor.ActorRef]:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #ask-pipeTo }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #import-ask #ask-pipe }


This example demonstrates `ask` together with the @scala[@scaladoc[pipeTo](akka.pattern.PipeToSupport$PipeableFuture#pipeTo(recipient:akka.actor.ActorRef)(implicitsender:akka.actor.ActorRef):scala.concurrent.Future[T])]@java[@javadoc[pipeTo](akka.pattern.PipeToSupport.PipeableCompletionStage#pipeTo(akka.actor.ActorRef,akka.actor.ActorRef))] pattern on
futures, because this is likely to be a common combination. Please note that
all of the above is completely non-blocking and asynchronous: `ask` produces
a @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)], @scala[three] @java[two] of which are composed into a new future using the
@scala[for-comprehension and then `pipeTo` installs an `onComplete`-handler on the `Future` to affect]
@java[@javadoc[CompletableFuture.allOf](java.util.concurrent.CompletableFuture#allOf(java.util.concurrent.CompletableFuture...)) and @javadoc[thenApply](java.util.concurrent.CompletionStage#thenApply(java.util.function.Function)) methods and then `pipe` installs a handler on the `CompletionStage` to effect]
the submission of the aggregated `Result` to another actor.

Using `ask` will send a message to the receiving Actor as with `tell`, and
the receiving actor must reply with @scala[`sender() ! reply`] @java[`getSender().tell(reply, getSelf())` ] in order to
complete the returned @scala[`Future`]@java[`CompletionStage`] with a value. The `ask` operation involves creating
an internal actor for handling this reply, which needs to have a timeout after
which it is destroyed in order not to leak resources; see more below.

@@@ warning

To complete the @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)] with an exception you need to send an @apidoc[akka.actor.Status.Failure] message to the sender.
This is *not done automatically* when an actor throws an exception while processing a message.

@scala[Please note that Scala's `Try` sub types @scaladoc[scala.util.Failure](scala.util.Failure) and @scaladoc[scala.util.Success](scala.util.Success) are not treated
especially, and would complete the ask @scala[`Future`]@java[`CompletionStage`] with the given value - only the @scaladoc[akka.actor.Status](akka.actor.Status$) messages
are treated specially by the ask pattern.]

@@@

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #reply-exception }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #reply-exception }

If the actor does not complete the @scala[@scaladoc[Future](scala.concurrent.Future)]@java[@javadoc[CompletionStage](java.util.concurrent.CompletionStage)], it will expire after the timeout period,
@scala[completing it with an @scaladoc[AskTimeoutException](akka.pattern.AskTimeoutException). The timeout is taken from one of the following locations in order of precedence:]
@java[specified as parameter to the `ask` method; this will complete the `CompletionStage` with an @javadoc[AskTimeoutException](akka.pattern.AskTimeoutException).]

@@@ div { .group-scala }

 1. explicitly given timeout as in:

    @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #using-explicit-timeout }

 2. implicit argument of type `akka.util.Timeout`, e.g.

    @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #using-implicit-timeout }

@@@

The @scala[@scaladoc[onComplete](scala.concurrent.Future#onComplete[U](f:scala.util.Try[T]=%3EU)(implicitexecutor:scala.concurrent.ExecutionContext):Unit) method of the `Future`]@java[@javadoc[thenRun](java.util.concurrent.CompletionStage#thenRun(java.lang.Runnable)) method of the `CompletionStage`] can be
used to register a callback to get a notification when the @scala[`Future`]@java[`CompletionStage`] completes, giving
you a way to avoid blocking.

@@@ warning

When using future callbacks, @scala[such as @scaladoc[onComplete](scala.concurrent.Future#onComplete[U](f:scala.util.Try[T]=%3EU)(implicitexecutor:scala.concurrent.ExecutionContext):Unit), or @scaladoc[map](scala.concurrent.Future#map[S](f:T=%3ES)(implicitexecutor:scala.concurrent.ExecutionContext):scala.concurrent.Future[S])]@scala[such as @javadoc[thenRun](java.util.concurrent.CompletionStage#thenRun(java.lang.Runnable)), or @javadoc[thenApply](java.util.concurrent.CompletionStage#thenApply(java.util.function.Function))]
inside actors you need to carefully avoid closing over
the containing actor’s reference, i.e. do not call methods or access mutable state
on the enclosing actor from within the callback. This would break the actor
encapsulation and may introduce synchronization bugs and race conditions because
the callback will be scheduled concurrently to the enclosing actor. Unfortunately,
there is not yet a way to detect these illegal accesses at compile time.
See also: @ref:[Actors and shared mutable state](general/jmm.md#jmm-shared-state)

@@@

### Forward message

You can forward a message from one actor to another. This means that the
original sender address/reference is maintained even though the message is going
through a 'mediator'. This can be useful when writing actors that work as
routers, load-balancers, replicators etc.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #forward }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #forward }

## Receive messages

An Actor has to
@scala[implement the @scaladoc[receive](akka.actor.Actor#receive:akka.actor.Actor.Receive) method to receive messages:]
@java[define its initial receive behavior by implementing the @javadoc[createReceive](akka.actor.AbstractActor#createReceive()) method in the `AbstractActor`:]

Scala
:  @@snip [Actor.scala](/akka-actor/src/main/scala/akka/actor/Actor.scala) { #receive }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #createReceive }

@@@ div { .group-scala }

This method returns a `PartialFunction`, e.g. a ‘match/case’ clause in
which the message can be matched against the different case clauses using Scala
pattern matching. Here is an example:

@@@

@@@ div { .group-java }

The return type is @javadoc[AbstractActor.Receive](akka.actor.AbstractActor.Receive) that defines which messages your Actor can handle,
along with the implementation of how the messages should be processed.
You can build such behavior with a builder named @javadoc[receiveBuilder](akka.actor.AbstractActor#receiveBuilder()). Here is an example:

@@@

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #imports1 #my-actor }

Java
:  @@snip [MyActor.java](/akka-docs/src/test/java/jdocs/actor/MyActor.java) { #imports #my-actor }

@@@ div { .group-java }

In case you want to provide many `match` cases but want to avoid creating a long call
trail, you can split the creation of the builder into multiple statements as in the example:

@@snip [GraduallyBuiltActor.java](/akka-docs/src/test/java/jdocs/actor/GraduallyBuiltActor.java) { #imports #actor }

Using small methods is a good practice, also in actors. It's recommended to delegate the
actual work of the message processing to methods instead of defining a huge `ReceiveBuilder`
with lots of code in each lambda. A well-structured actor can look like this:

@@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #well-structured }

That has benefits such as:

* easier to see what kind of messages the actor can handle
* readable stack traces in case of exceptions
* works better with performance profiling tools
* Java HotSpot has a better opportunity for making optimizations

The @javadoc[Receive](akka.actor.AbstractActor.Receive) can be implemented in other ways than using the `ReceiveBuilder` since in the
end, it is just a wrapper around a Scala `PartialFunction`. In Java, you can implement `PartialFunction` by
extending `AbstractPartialFunction`.

If the validation of the `ReceiveBuilder` match logic turns out to be a bottleneck for some of your
actors you can consider implementing it at a lower level by extending @javadoc[UntypedAbstractActor](akka.actor.UntypedAbstractActor) instead
of @javadoc[AbstractActor](akka.actor.AbstractActor). The partial functions created by the `ReceiveBuilder` consist of multiple lambda
expressions for every match statement, where each lambda is referencing the code to be run. This is something
that the JVM can have problems optimizing and the resulting code might not be as performant as the
untyped version. When extending `UntypedAbstractActor` each message is received as an untyped
`Object` and you have to inspect and cast it to the actual message type in other ways, like this:

@@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #optimized }

@@@

<a id="actor-reply"></a>
## Reply to messages

If you want to have a handle for replying to a message, you can use
@scala[@scaladoc[sender](akka.actor.Actor#sender():akka.actor.ActorRef)] @java[@javadoc[getSender()](akka.actor.AbstractActor#getSender())], which gives you an ActorRef. You can reply by sending to
that ActorRef with @scala[`sender() ! replyMsg`.] @java[`getSender().tell(replyMsg, getSelf())`.] You can also store the ActorRef
for replying later, or passing it on to other actors. If there is no sender (a
message was sent without an actor or future context) then the sender
defaults to a 'dead-letter' actor ref.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #reply-without-sender }

Java
:  @@snip [MyActor.java](/akka-docs/src/test/java/jdocs/actor/MyActor.java) { #reply }

## Receive timeout

The @java[@scaladoc[ActorContext.setReceiveTimeout](akka.actor.ActorContext#setReceiveTimeout(timeout:scala.concurrent.duration.Duration):Unit)]@java[@javadoc[ActorContext.setReceiveTimeout](akka.actor.AbstractActor.ActorContext#setReceiveTimeout(java.time.Duration))] defines the inactivity timeout after which
the sending of a @apidoc[akka.actor.ReceiveTimeout] message is triggered.
When specified, the receive function should be able to handle an `akka.actor.ReceiveTimeout` message.
1 millisecond is the minimum supported timeout.

Please note that the receive timeout might fire and enqueue the `ReceiveTimeout` message right after
another message was enqueued; hence it is **not guaranteed** that upon reception of the receive
timeout there must have been an idle period beforehand as configured via this method.

Once set, the receive timeout stays in effect (i.e. continues firing repeatedly after inactivity
periods).

To cancel the sending of receive timeout notifications, use `cancelReceiveTimeout`.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #receive-timeout }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #receive-timeout }

Messages marked with @apidoc[NotInfluenceReceiveTimeout] will not reset the timer. This can be useful when
@apidoc[akka.actor.ReceiveTimeout] should be fired by external inactivity but not influenced by internal activity,
e.g. scheduled tick messages.

<a id="actors-timers"></a>

## Timers, scheduled messages

Messages can be scheduled to be sent at a later point by using the @ref:[Scheduler](scheduler.md) directly,
but when scheduling periodic or single messages in an actor to itself it's more convenient and safe
to use the support for named timers. The lifecycle of scheduled messages can be difficult to manage
when the actor is restarted and that is taken care of by the timers.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/TimerDocSpec.scala) { #timers }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/TimerDocTest.java) { #timers }

The @ref:[Scheduler](scheduler.md#schedule-periodically) documentation describes the difference between
`fixed-delay` and `fixed-rate` scheduling. If you are uncertain of which one to use you should pick
@apidoc[startTimerWithFixedDelay](akka.actor.TimerScheduler) {scala="#startTimerWithFixedDelay(key:Any,msg:Any,initialDelay:scala.concurrent.duration.FiniteDuration,delay:scala.concurrent.duration.FiniteDuration):Unit" java="#startTimerWithFixedDelay(java.lang.Object,java.lang.Object,java.time.Duration,java.time.Duration)"}.

Each timer has a key and can be replaced or cancelled. It's guaranteed that a message from the
previous incarnation of the timer with the same key is not received, even though it might already
be enqueued in the mailbox when it was cancelled or the new timer was started.

The timers are bound to the lifecycle of the actor that owns it and thus are cancelled
automatically when it is restarted or stopped. Note that the @apidoc[akka.actor.TimerScheduler] is not thread-safe,
i.e. it must only be used within the actor that owns it.

## Stopping actors

Actors are stopped by invoking the `stop` method of a @apidoc[akka.actor.ActorRefFactory],
i.e. @apidoc[akka.actor.ActorContext] or @apidoc[akka.actor.ActorSystem]. Typically the context is used for stopping
the actor itself or child actors and the system for stopping top-level actors. The actual
termination of the actor is performed asynchronously, i.e. `stop` may return before
the actor is stopped.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #stoppingActors-actor }

Java
:  @@snip [MyStoppingActor.java](/akka-docs/src/test/java/jdocs/actor/MyStoppingActor.java) { #my-stopping-actor }


Processing of the current message, if any, will continue before the actor is stopped,
but additional messages in the mailbox will not be processed. By default, these
messages are sent to the @apidoc[deadLetters](akka.actor.ActorSystem) {scala="#deadLetters:akka.actor.ActorRef" java="#deadLetters()"} of the `ActorSystem`, but that
depends on the mailbox implementation.

Termination of an actor proceeds in two steps: first the actor suspends its
mailbox processing and sends a stop command to all its children, then it keeps
processing the internal termination notifications from its children until the last one is
gone, finally terminating itself (invoking @scala[@scaladoc[postStop](akka.actor.Actor#postStop():Unit)]@java[@javadoc[postStop](akka.actor.AbstractActor#postStop())], dumping mailbox,
publishing @apidoc[akka.actor.Terminated] on the @ref:[DeathWatch](#deathwatch), telling
its supervisor). This procedure ensures that actor system sub-trees terminate
in an orderly fashion, propagating the stop command to the leaves and
collecting their confirmation back to the stopped supervisor. If one of the
actors do not respond (i.e. processing a message for extended periods of time
and therefore not receiving the stop command), this whole process will be
stuck.

Upon @apidoc[ActorSystem.terminate()](akka.actor.ActorSystem) {scala="#terminate():scala.concurrent.Future[akka.actor.Terminated]" java="#terminate()"}, the system guardian actors will be
stopped, and the aforementioned process will ensure proper termination of the
whole system. See @ref:[Coordinated Shutdown](coordinated-shutdown.md).

The `postStop()` hook is invoked after an actor is fully stopped. This
enables cleaning up of resources:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #postStop }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #postStop }

@@@ note

Since stopping an actor is asynchronous, you cannot immediately reuse the
name of the child you just stopped; this will result in an
@apidoc[akka.actor.InvalidActorNameException]. Instead, `watch()` the terminating
actor and create its replacement in response to the @apidoc[akka.actor.Terminated]
message which will eventually arrive.

@@@

<a id="poison-pill"></a>
### PoisonPill

You can also send an actor the @apidoc[akka.actor.PoisonPill] message, which will
stop the actor when the message is processed. `PoisonPill` is enqueued as
ordinary messages and will be handled after messages that were already queued
in the mailbox.

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #poison-pill }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #poison-pill }

<a id="killing-actors"></a>
### Killing an Actor

You can also "kill" an actor by sending a @apidoc[akka.actor.Kill] message. Unlike `PoisonPill` this will cause the actor to throw a @apidoc[akka.actor.ActorKilledException], triggering a failure. The actor will
suspend operation and its supervisor will be asked how to handle the failure,
which may mean resuming the actor, restarting it or terminating it completely.
See @ref:[What Supervision Means](general/supervision.md#supervision-directives) for more information.

Use `Kill` like this:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #kill }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #kill }

In general, it is not recommended to overly rely on either `PoisonPill` or `Kill` in
designing your actor interactions, as often a protocol-level message like `PleaseCleanupAndStop`
which the actor knows how to handle is encouraged. The messages are there for being able to stop actors
over which design you do not have control over.

### Graceful Stop

@scala[@scaladoc[gracefulStop](akka.pattern.GracefulStopSupport#gracefulStop(target:akka.actor.ActorRef,timeout:scala.concurrent.duration.FiniteDuration,stopMessage:Any):scala.concurrent.Future[Boolean])]@java[@javadoc[gracefulStop](akka.pattern.Patterns#gracefulStop(akka.actor.ActorRef,scala.concurrent.duration.FiniteDuration,java.lang.Object))] is useful if you need to wait for termination or compose ordered
termination of several actors:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #gracefulStop}

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #import-gracefulStop #gracefulStop}

When `gracefulStop()` returns successfully, the actor’s @scala[@scaladoc[postStop](akka.actor.Actor#postStop():Unit)]@java[@javadoc[postStop](akka.actor.AbstractActor#postStop())] hook
will have been executed: there exists a happens-before edge between the end of
`postStop()` and the return of `gracefulStop()`.

In the above example, a custom `Manager.Shutdown` message is sent to the target
actor to initiate the process of stopping the actor. You can use `PoisonPill` for
this, but then you have limited possibilities to perform interactions with other actors
before stopping the target actor. Simple cleanup tasks can be handled in `postStop`.

@@@ warning

Keep in mind that an actor stopping and its name being deregistered are
separate events that happen asynchronously from each other. Therefore it may
be that you will find the name still in use after `gracefulStop()`
returned. To guarantee proper deregistration, only reuse names from
within a supervisor you control and only in response to a @apidoc[akka.actor.Terminated]
message, i.e. not for top-level actors.

@@@

<a id="actor-hotswap"></a>
## Become/Unbecome

### Upgrade

Akka supports hotswapping the Actor’s message loop (e.g. its implementation) at
runtime: invoke the @apidoc[context.become](akka.actor.ActorContext) {scala="#become(behavior:akka.actor.Actor.Receive,discardOld:Boolean):Unit" java="#become(scala.PartialFunction,boolean)"} method from within the Actor.
`become` takes a @scala[`PartialFunction[Any, Unit]`] @java[`PartialFunction<Object, BoxedUnit>`] that implements the new
message handler. The hotswapped code is kept in a Stack that can be pushed and
popped.

@@@ warning

Please note that the actor will revert to its original behavior when restarted by its Supervisor.

@@@

To hotswap the Actor behavior using `become`:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #hot-swap-actor }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #hot-swap-actor }

This variant of the `become` method is useful for many different things,
such as to implement a Finite State Machine (FSM). It will replace the current behavior (i.e. the top of the behavior
stack), which means that you do not use @apidoc[unbecome](akka.actor.ActorContext) {scala="#unbecome():Unit" java="#unbecome()"}, instead always the
next behavior is explicitly installed.

The other way of using `become` does not replace but add to the top of
the behavior stack. In this case, care must be taken to ensure that the number
of “pop” operations (i.e. `unbecome`) matches the number of “push” ones
in the long run, otherwise this amounts to a memory leak (which is why this
behavior is not the default).

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #swapper }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #swapper }

### Encoding Scala Actors nested receives without accidentally leaking memory

See this @extref[Unnested receive example](github:akka-docs/src/test/scala/docs/actor/UnnestedReceives.scala).

## Stash

The @scala[@scaladoc[Stash](akka.actor.Stash) trait] @java[@javadoc[AbstractActorWithStash](akka.actor.AbstractActorWithStash) class] enables an actor to temporarily stash away messages
that can not or should not be handled using the actor's current
behavior. Upon changing the actor's message handler, i.e., right
before invoking @scala[`context.become` or `context.unbecome`] @java[`getContext().become()` or `getContext().unbecome()`], all
stashed messages can be "unstashed", thereby prepending them to the actor's
mailbox. This way, the stashed messages can be processed in the same
order as they have been received originally. @java[An actor that extends
`AbstractActorWithStash` will automatically get a deque-based mailbox.]

@@@ note  { .group-scala }

The trait `Stash` extends the marker trait
@scaladoc[RequiresMessageQueue](akka.dispatch.RequiresMessageQueue)[@scaladoc[DequeBasedMessageQueueSemantics](akka.dispatch.DequeBasedMessageQueueSemantics)] which
requests the system to automatically choose a deque based
mailbox implementation for the actor. If you want more
control over the
mailbox, see the documentation on mailboxes: @ref:[Mailboxes](mailboxes.md).

@@@

@@@ note { .group-java }

The abstract class @javadoc[AbstractActorWithStash](akka.actor.AbstractActorWithStash) implements the marker
interface @javadoc[RequiresMessageQueue](akka.dispatch.RequiresMessageQueue)<@javadoc[DequeBasedMessageQueueSemantics](akka.dispatch.DequeBasedMessageQueueSemantics)>
which requests the system to automatically choose a deque based
mailbox implementation for the actor. If you want more
control over the mailbox, see the documentation on mailboxes: @ref:[Mailboxes](mailboxes.md).

@@@

Here is an example of the @scala[@scaladoc[Stash](akka.actor.Stash) trait] @java[@javadoc[AbstractActorWithStash](akka.actor.AbstractActorWithStash) class] in action:

Scala
:  @@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #stash }

Java
:  @@snip [ActorDocTest.java](/akka-docs/src/test/java/jdocs/actor/ActorDocTest.java) { #stash }

Invoking `stash()` adds the current message (the message that the
actor received last) to the actor's stash. It is typically invoked
when handling the default case in the actor's message handler to stash
messages that aren't handled by the other cases. It is illegal to
stash the same message twice; to do so results in an
@javadoc[IllegalStateException](java.lang.IllegalStateException) being thrown. The stash may also be bounded
in which case invoking `stash()` may lead to a capacity violation,
which results in a @apidoc[akka.actor.StashOverflowException]. The capacity of the
stash can be configured using the `stash-capacity` setting (an `Int`) of the
mailbox's configuration.

Invoking `unstashAll()` enqueues messages from the stash to the
actor's mailbox until the capacity of the mailbox (if any) has been
reached (note that messages from the stash are prepended to the
mailbox). In case a bounded mailbox overflows, a
`MessageQueueAppendFailedException` is thrown.
The stash is guaranteed to be empty after calling `unstashAll()`.

The stash is backed by a @scaladoc[scala.collection.immutable.Vector](scala.collection.immutable.Vector). As a
result, even a very large number of messages may be stashed without a
major impact on performance.

@@@ warning { .group-scala }

Note that the @scaladoc[Stash](akka.actor.Stash) trait must be mixed into (a subclass of) the
@scaladoc[Actor](akka.actor.Actor) trait before any trait/class that overrides the @scaladoc[preRestart](akka.actor.Actor#preRestart(reason:Throwable,message:Option[Any]):Unit)]
callback. This means it's not possible to write
`Actor with MyActor with Stash` if `MyActor` overrides `preRestart`.

@@@

Note that the stash is part of the ephemeral actor state, unlike the
mailbox. Therefore, it should be managed like other parts of the
actor's state which have the same property.

However, the @scala[@scaladoc[Stash](akka.actor.Stash) trait] @java[@javadoc[AbstractActorWithStash](akka.actor.AbstractActorWithStash) class]
implementation of `preRestart` will call `unstashAll()`. This means
that before the actor restarts, it will transfer all stashed messages back to the actor's mailbox.

The result of this is that when an actor is restarted, any stashed messages will be delivered to the new incarnation of the actor.
This is usually the desired behavior.

@@@ note

If you want to enforce that your actor can only work with an unbounded stash,
then you should use the @scala[@scaladoc[UnboundedStash](akka.actor.UnboundedStash) trait] @java[@javadoc[AbstractActorWithUnboundedStash](akka.actor.AbstractActorWithUnboundedStash) class] instead.

@@@

@@@ div { .group-scala }

## Extending Actors using PartialFunction chaining

Sometimes it can be useful to share common behavior among a few actors, or compose one actor's behavior from multiple smaller functions.
This is possible because an actor's @scala[@scaladoc[receive](akka.actor.Actor#receive:akka.actor.Actor.Receive)]@java[@javadoc[createReceive](akka.actor.AbstractActor#createReceive())] method returns an `Actor.Receive`, which is a type alias for `PartialFunction[Any,Unit]`,
and partial functions can be chained together using the `PartialFunction#orElse` method. You can chain as many functions as you need,
however you should keep in mind that "first match" wins - which may be important when combining functions that both can handle the same type of message.

For example, imagine you have a set of actors which are either `Producers` or `Consumers`, yet sometimes it makes sense to
have an actor share both behaviors. This can be achieved without having to duplicate code by extracting the behaviors to
traits and implementing the actor's `receive` as a combination of these partial functions.

@@snip [ActorDocSpec.scala](/akka-docs/src/test/scala/docs/actor/ActorDocSpec.scala) { #receive-orElse }

Instead of inheritance the same pattern can be applied via composition - compose the receive method using partial functions from delegates.

@@@

## Initialization patterns

The rich lifecycle hooks of Actors provide a useful toolkit to implement various initialization patterns. During the
lifetime of an @apidoc[akka.actor.ActorRef], an actor can potentially go through several restarts, where the old instance is replaced by
a fresh one, invisibly to the outside observer who only sees the `ActorRef`.

Initialization might be necessary every time an actor is instantiated,
but sometimes one needs initialization to happen only at the birth of the first instance when the
`ActorRef` is created. The following sections provide patterns for different initialization needs.

### Initialization via constructor

Using the constructor for initialization has various benefits. First of all, it makes it possible to use `val` fields to store
any state that does not change during the life of the actor instance, making the implementation of the actor more robust.
The constructor is invoked when an actor instance is created calling `actorOf` and also on restart, therefore the internals of the actor can always assume
that proper initialization happened. This is also the drawback of this approach, as there are cases when one would
like to avoid reinitializing internals on restart. For example, it is often useful to preserve child actors across
restarts. The following section provides a pattern for this case.

### Initialization via preStart

The method @scala[@scaladoc[preStart](akka.actor.Actor#preStart():Unit)]@java[@javadoc[preStart](akka.actor.AbstractActor#preStart())] of an actor is only called once directly during the initialization of the first instance, that
is, at the creation of its `ActorRef`. In the case of restarts, `preStart()` is called from @scala[@scaladoc[postRestart](akka.actor.Actor#postRestart(reason:Throwable):Unit)]@java[@javadoc[postRestart](akka.actor.AbstractActor#postRestart(java.lang.Throwable))], therefore
if not overridden, `preStart()` is called on every restart. However, by overriding `postRestart()` one can disable
this behavior, and ensure that there is only one call to `preStart()`.

One useful usage of this pattern is to disable creation of new `ActorRefs` for children during restarts. This can be
achieved by overriding @scala[@scaladoc[preRestart](akka.actor.Actor#preRestart(reason:Throwable,message:Option[Any]):Unit)]@java[@javadoc[preRestart](akka.actor.AbstractActor#preRestart(java.lang.Throwable,java.util.Optional))]. Below is the default implementation of these lifecycle hooks:

Scala
:  @@snip [InitializationDocSpec.scala](/akka-docs/src/test/scala/docs/actor/InitializationDocSpec.scala) { #preStartInit }

Java
:  @@snip [InitializationDocTest.java](/akka-docs/src/test/java/jdocs/actor/InitializationDocTest.java) { #preStartInit }


Please note, that the child actors are *still restarted*, but no new @apidoc[akka.actor.ActorRef] is created. One can recursively apply
the same principles for the children, ensuring that their `preStart()` method is called only at the creation of their
refs.

For more information see @ref:[What Restarting Means](general/supervision.md#supervision-restart).

### Initialization via message passing

There are cases when it is impossible to pass all the information needed for actor initialization in the constructor,
for example in the presence of circular dependencies. In this case, the actor should listen for an initialization message,
and use @apidoc[become()](akka.actor.ActorContext) {scala="#become(behavior:akka.actor.Actor.Receive,discardOld:Boolean):Unit" java="#become(scala.PartialFunction,boolean)"} or a finite state-machine state transition to encode the initialized and uninitialized states
of the actor.

Scala
:  @@snip [InitializationDocSpec.scala](/akka-docs/src/test/scala/docs/actor/InitializationDocSpec.scala) { #messageInit }

Java
:  @@snip [InitializationDocTest.java](/akka-docs/src/test/java/jdocs/actor/InitializationDocTest.java) { #messageInit }

If the actor may receive messages before it has been initialized, a useful tool can be the `Stash` to save messages
until the initialization finishes, and replaying them after the actor became initialized.

@@@ warning

This pattern should be used with care, and applied only when none of the patterns above are applicable. One of
the potential issues is that messages might be lost when sent to remote actors. Also, publishing an @apidoc[akka.actor.ActorRef] in
an uninitialized state might lead to the condition that it receives a user message before the initialization has been
done.

@@@
