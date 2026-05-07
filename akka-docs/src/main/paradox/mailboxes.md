# Classic Mailboxes

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[mailboxes](typed/mailboxes.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Mailboxes, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

An Akka `Mailbox` holds the messages that are destined for an @apidoc[akka.actor.Actor].
Normally each `Actor` has its own mailbox, but with for example a @apidoc[BalancingPool]
all routees will share a single mailbox instance.

For more details on advanced mailbox config and custom mailbox implementations, see @ref[Mailboxes](typed/mailboxes.md#mailbox-implementations).

## Mailbox Selection

### Default Mailbox

The default mailbox is used when the mailbox is not specified.
This is an unbounded mailbox, backed by a
@javadoc[java.util.concurrent.ConcurrentLinkedQueue](java.util.concurrent.ConcurrentLinkedQueue).

@apidoc[SingleConsumerOnlyUnboundedMailbox] is an even more efficient mailbox, and
it can be used as the default mailbox, but it cannot be used with a BalancingDispatcher.

Configuration of `SingleConsumerOnlyUnboundedMailbox` as default mailbox:

```
akka.actor.default-mailbox {
  mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
}
```

### Requiring a Message Queue Type for an Actor

It is possible to require a certain type of message queue for a certain type of actor
by having that actor @scala[extend]@java[implement] the parameterized @scala[trait]@java[interface] @apidoc[RequiresMessageQueue]. Here is
an example:

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #required-mailbox-class }

Java
:   @@snip [MyBoundedActor.java](/akka-docs/src/test/java/jdocs/actor/MyBoundedActor.java) { #my-bounded-classic-actor }

The type parameter to the `RequiresMessageQueue` @scala[trait]@java[interface] needs to be mapped to a mailbox in
configuration like this:

@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #bounded-mailbox-config #required-mailbox-config }

Now every time you create an actor of type `MyBoundedActor` it will try to get a bounded
mailbox. If the actor has a different mailbox configured in deployment, either directly or via
a dispatcher with a specified mailbox type, then that will override this mapping.

@@@ note

The type of the queue in the mailbox created for an actor will be checked against the required type in the
@scala[trait]@java[interface] and if the queue doesn't implement the required type then actor creation will fail.

@@@

### Requiring a Message Queue Type for a Dispatcher

A dispatcher may also have a requirement for the mailbox type used by the
actors running on it. An example is the `BalancingDispatcher` which requires a
message queue that is thread-safe for multiple concurrent consumers. Such a
requirement is formulated within the dispatcher configuration section:

```
my-dispatcher {
  mailbox-requirement = org.example.MyInterface
}
```

The given requirement names a class or interface which will then be ensured to
be a supertype of the message queue's implementation. In case of a
conflict—e.g. if the actor requires a mailbox type which does not satisfy this
requirement—then actor creation will fail.

### How the Mailbox Type is Selected

When an actor is created, the @apidoc[ActorRefProvider] first determines the
dispatcher which will execute it. Then the mailbox is determined as follows:

 1. If the actor's deployment configuration section contains a `mailbox` key,
this refers to a configuration section describing the mailbox type.
 2. If the actor's @apidoc[Props](akka.actor.Props) contains a mailbox selection then that names a configuration section describing the
mailbox type to be used. This needs to be an absolute config path,
for example `myapp.special-mailbox`, and is not nested inside the `akka` namespace.
 3. If the dispatcher's configuration section contains a `mailbox-type` key
the same section will be used to configure the mailbox type.
 4. If the actor requires a mailbox type as described above then the mapping for
that requirement will be used to determine the mailbox type to be used; if
that fails then the dispatcher's requirement—if any—will be tried instead.
 5. If the dispatcher requires a mailbox type as described above then the
mapping for that requirement will be used to determine the mailbox type to
be used.
 6. The default mailbox `akka.actor.default-mailbox` will be used.

## Mailbox configuration examples

### PriorityMailbox

How to create a PriorityMailbox:

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #prio-mailbox }

Java
:   @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #prio-mailbox }

And then add it to the configuration:

@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #prio-dispatcher-config }

And then an example on how you would use it:

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #prio-dispatcher }

Java
:   @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #prio-dispatcher }

It is also possible to configure a mailbox type directly like this (this is a top-level configuration entry):

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #prio-mailbox-config #mailbox-deployment-config }

Java
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #prio-mailbox-config-java #mailbox-deployment-config }

And then use it either from deployment like this:

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #defining-mailbox-in-config }

Java
:   @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #defining-mailbox-in-config }

Or code like this:

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #defining-mailbox-in-code }

Java
:   @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #defining-mailbox-in-code }

### ControlAwareMailbox

A `ControlAwareMailbox` can be very useful if an actor needs to be able to receive control messages
immediately no matter how many other messages are already in its mailbox.

It can be configured like this:

@@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #control-aware-mailbox-config }

Control messages need to extend the @apidoc[akka.dispatch.ControlMessage] @scala[trait]@java[interface]:

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #control-aware-mailbox-messages }

Java
:   @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #control-aware-mailbox-messages }

And then an example on how you would use it:

Scala
:   @@snip [DispatcherDocSpec.scala](/akka-docs/src/test/scala/docs/dispatcher/DispatcherDocSpec.scala) { #control-aware-dispatcher }

Java
:   @@snip [DispatcherDocTest.java](/akka-docs/src/test/java/jdocs/dispatcher/DispatcherDocTest.java) { #control-aware-dispatcher }

## Special Semantics of `system.actorOf`

In order to make @apidoc[system.actorOf](akka.actor.ActorRefFactory) {scala="#actorOf(props:akka.actor.Props):akka.actor.ActorRef" java="#actorOf(akka.actor.Props)"} both synchronous and non-blocking while
keeping the return type @apidoc[akka.actor.ActorRef] (and the semantics that the returned
ref is fully functional), special handling takes place for this case. Behind
the scenes, a hollow kind of actor reference is constructed, which is sent to
the system’s guardian actor who actually creates the actor and its context and
puts those inside the reference. Until that has happened, messages sent to the
@apidoc[akka.actor.ActorRef] will be queued locally, and only upon swapping the real
filling in will they be transferred into the real mailbox. Thus,

Scala
:   @@@vars
    ```scala
    val props: Props = ...
    // this actor uses MyCustomMailbox, which is assumed to be a singleton
    system.actorOf(props.withDispatcher("myCustomMailbox")) ! "bang"
    assert(MyCustomMailbox.instance.getLastEnqueuedMessage == "bang")
    ```
    @@@

Java
:   @@@vars
    ```java
    final Props props = ...
    // this actor uses MyCustomMailbox, which is assumed to be a singleton
    system.actorOf(props.withDispatcher("myCustomMailbox").tell("bang", sender);
    assert(MyCustomMailbox.getInstance().getLastEnqueued().equals("bang"));
    ```
    @@@

will probably fail; you will have to allow for some time to pass and retry the
check à la @scala[@scaladoc[TestKit.awaitCond](akka.testkit.TestKit#awaitCond(p:=%3EBoolean,max:scala.concurrent.duration.Duration,interval:scala.concurrent.duration.Duration,message:String):Unit)]@java[@javadoc[TestKit.awaitCond](akka.testkit.javadsl.TestKit#awaitCond(java.time.Duration,java.time.Duration,java.lang.String,java.util.function.Supplier))].
