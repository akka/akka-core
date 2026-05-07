---
project.description: Akka Persistence Classic, Event Sourcing with Akka, At-Least-Once delivery, snapshots, recovery and replay with Akka actors.
---
# Classic Persistence

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[Event Sourcing](typed/persistence.md).

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Persistence, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-persistence_$scala.binary.version$"
  version=AkkaVersion
  group2="com.typesafe.akka"
  artifact2="akka-persistence-testkit_$scala.binary.version$"
  version2=AkkaVersion
  scope2=test
}

You also have to select journal plugin and optionally snapshot store plugin, see 
@ref:[Persistence Plugins](persistence-plugins.md).

@@project-info{ projectId="akka-persistence" }

## Introduction

See introduction in @ref:[Persistence](typed/persistence.md#introduction) 

Akka Persistence also provides point-to-point communication with at-least-once message delivery semantics.

### Architecture

 * @scala[@scaladoc[PersistentActor](akka.persistence.PersistentActor)]@java[@javadoc[AbstractPersistentActor](akka.persistence.AbstractPersistentActor)]: Is a persistent, stateful actor. It is able to persist events to a journal and can react to
them in a thread-safe manner. It can be used to implement both *command* as well as *event sourced* actors.
When a persistent actor is started or restarted, journaled messages are replayed to that actor so that it can
recover its state from these messages.
 * @scala[@scaladoc[AtLeastOnceDelivery](akka.persistence.AtLeastOnceDelivery)]@java[@javadoc[AbstractPersistentActorWithAtLeastOnceDelivery](akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery)]: To send messages with at-least-once delivery semantics to destinations, also in
case of sender and receiver JVM crashes.
 * @scala[@scaladoc[AsyncWriteJournal](akka.persistence.journal.AsyncWriteJournal)]@java[@javadoc[AsyncWriteJournal](akka.persistence.journal.japi.AsyncWriteJournal)]: A journal stores the sequence of messages sent to a persistent actor. An application can control which messages
are journaled and which are received by the persistent actor without being journaled. Journal maintains `highestSequenceNr` that is increased on each message.
The storage backend of a journal is pluggable. 
Replicated journals are available as [Community plugins](https://akka.io/community/).
 * *Snapshot store*: A snapshot store persists snapshots of a persistent actor's state. Snapshots are
used for optimizing recovery times. The storage backend of a snapshot store is pluggable.
The persistence extension comes with a "local" snapshot storage plugin, which writes to the local filesystem. Replicated snapshot stores are available as [Community plugins](https://akka.io/community/)
 * *Event Sourcing*. Based on the building blocks described above, Akka persistence provides abstractions for the
development of event sourced applications (see section @ref:[Event Sourcing](typed/persistence.md#event-sourcing-concepts)).

## Example

Akka persistence supports Event Sourcing with the @scala[@scaladoc[PersistentActor](akka.persistence.PersistentActor) trait]@java[@javadoc[AbstractPersistentActor](akka.persistence.AbstractPersistentActor) abstract class]. An actor that extends this @scala[trait]@java[class] uses the
@scala[@scaladoc[persist](akka.persistence.PersistentActor#persist[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persist](akka.persistence.AbstractPersistentActorLike#persist(A,akka.japi.Procedure))] method to persist and handle events. The behavior of @scala[a `PersistentActor`]@java[an `AbstractPersistentActor`]
is defined by implementing @scala[@scaladoc[receiveRecover](akka.persistence.PersistentActor#receiveRecover:Eventsourced.this.Receive)]@java[@javadoc[createReceiveRecover](akka.persistence.AbstractPersistentActorLike#createReceiveRecover())] and @scala[@scaladoc[receiveCommand](akka.persistence.PersistentActor#receiveCommand:Eventsourced.this.Receive)]@java[@javadoc[createReceive](akka.persistence.AbstractPersistentActorLike#createReceive())]. This is demonstrated in the following example.

Scala
:  @@snip [PersistentActorExample.scala](/akka-docs/src/test/scala/docs/persistence/PersistentActorExample.scala) { #persistent-actor-example }

Java
:  @@snip [PersistentActorExample.java](/akka-docs/src/test/java/jdocs/persistence/PersistentActorExample.java) { #persistent-actor-example }

The example defines two data types, `Cmd` and `Evt` to represent commands and events, respectively. The
`state` of the `ExamplePersistentActor` is a list of persisted event data contained in `ExampleState`.

The persistent actor's @scala[`receiveRecover`]@java[`createReceiveRecover`] method defines how `state` is updated during recovery by handling `Evt`
and `SnapshotOffer` messages. The persistent actor's @scala[`receiveCommand`]@java[`createReceive`] method is a command handler. In this example,
a command is handled by generating an event which is then persisted and handled. Events are persisted by calling
`persist` with an event (or a sequence of events) as first argument and an event handler as second argument.

The `persist` method persists events asynchronously and the event handler is executed for successfully persisted
events. Successfully persisted events are internally sent back to the persistent actor as individual messages that trigger
event handler executions. An event handler may close over persistent actor state and mutate it. The sender of a persisted
event is the sender of the corresponding command. This allows event handlers to reply to the sender of a command
(not shown).

The main responsibility of an event handler is changing persistent actor state using event data and notifying others
about successful state changes by publishing events.

When persisting events with `persist` it is guaranteed that the persistent actor will not receive further commands between
the `persist` call and the execution(s) of the associated event handler. This also holds for multiple `persist`
calls in context of a single command. Incoming messages are @ref:[stashed](#internal-stash) until the `persist`
is completed.

If persistence of an event fails, `onPersistFailure` will be invoked (logging the error by default),
and the actor will unconditionally be stopped. If persistence of an event is rejected before it is
stored, e.g. due to serialization error, `onPersistRejected` will be invoked (logging a warning
by default) and the actor continues with the next message.

@@@ note

It's also possible to switch between different command handlers during normal processing and recovery
with @scala[@scaladoc[context.become()](akka.actor.ActorContext#become(behavior:akka.actor.Actor.Receive):Unit)]@java[@javadoc[getContext().become()](akka.actor.ActorContext#become(scala.PartialFunction))] and @scala[@scaladoc[context.unbecome()](akka.actor.ActorContext#unbecome():Unit)]@java[@javadoc[getContext().unbecome()](akka.actor.ActorContext#unbecome())]. To get the actor into the same state after
recovery you need to take special care to perform the same state transitions with `become` and
`unbecome` in the @scala[@scaladoc[receiveRecover](akka.persistence.PersistentActor#receiveRecover:Eventsourced.this.Receive)]@java[@javadoc[createReceiveRecover](akka.persistence.AbstractPersistentActorLike#createReceiveRecover())] method as you would have done in the command handler.
Note that when using `become` from @scala[`receiveRecover`]@java[`createReceiveRecover`] it will still only use the @scala[`receiveRecover`]@java[`createReceiveRecover`]
behavior when replaying the events. When replay is completed it will use the new behavior.

@@@

<a id="persistence-id"></a>
### Identifiers

A persistent actor must have an identifier that doesn't change across different actor incarnations.
The identifier must be defined with the `persistenceId` method.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #persistence-id-override }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #persistence-id-override }

@@@ note

`persistenceId` must be unique to a given entity in the journal (database table/keyspace).
When replaying messages persisted to the journal, you query messages with a `persistenceId`.
So, if two different entities share the same `persistenceId`, message-replaying
behavior is corrupted.

@@@

### Recovery

By default, a persistent actor is automatically recovered on start and on restart by replaying journaled messages.
New messages sent to a persistent actor during recovery do not interfere with replayed messages.
They are stashed and received by a persistent actor after recovery phase completes.

The number of concurrent recoveries that can be in progress at the same time is limited
to not overload the system and the backend data store. When exceeding the limit the actors will wait
until other recoveries have been completed. This is configured by:

```
akka.persistence.max-concurrent-recoveries = 50
```

@@@ note

Accessing the @scala[`sender()`]@java[sender with `getSender()`] for replayed messages will always result in a `deadLetters` reference,
as the original sender is presumed to be long gone. If you indeed have to notify an actor during
recovery in the future, store its @apidoc[akka.actor.ActorPath] explicitly in your persisted events.

@@@

<a id="recovery-custom"></a>
#### Recovery customization

Applications may also customise how recovery is performed by returning a customised @apidoc[akka.persistence.Recovery$] object
in the `recovery` method of a @scala[@scaladoc[PersistentActor](akka.persistence.PersistentActor)]@java[@javadoc[AbstractPersistentActor](akka.persistence.AbstractPersistentActor)],

To skip loading snapshots and replay all events you can use @scala[@scaladoc[SnapshotSelectionCriteria.None](akka.persistence.SnapshotSelectionCriteria$#None:akka.persistence.SnapshotSelectionCriteria)]@java[@javadoc[SnapshotSelectionCriteria.none()](akka.persistence.SnapshotSelectionCriteria#none())].
This can be useful if snapshot serialization format has changed in an incompatible way.
It should typically not be used when events have been deleted.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #recovery-no-snap }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #recovery-no-snap }

Another possible recovery customization, which can be useful for debugging, is setting an
upper bound on the replay, causing the actor to be replayed only up to a certain point "in the past" (instead of being replayed to its most up to date state). Note that after that it is a bad idea to persist new
events because a later recovery will probably be confused by the new events that follow the
events that were previously skipped.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #recovery-custom }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #recovery-custom }

Recovery can be disabled by returning `Recovery.none()` in the `recovery` method of a `PersistentActor`:

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #recovery-disabled }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #recovery-disabled }

#### Recovery status

A persistent actor can query its own recovery status via the methods

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #recovery-status }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #recovery-status }

Sometimes there is a need for performing additional initialization when the
recovery has completed before processing any other message sent to the persistent actor.
The persistent actor will receive a special @apidoc[akka.persistence.RecoveryCompleted$] message right after recovery
and before any other received messages.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #recovery-completed }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #recovery-completed }

The actor will always receive a `RecoveryCompleted` message, even if there are no events
in the journal and the snapshot store is empty, or if it's a new persistent actor with a previously
unused `persistenceId`.

If there is a problem with recovering the state of the actor from the journal, `onRecoveryFailure`
is called (logging the error by default) and the actor will be stopped.

### Internal stash

The persistent actor has a private @ref:[stash](actors.md#stash) for internally caching incoming messages during
[recovery](#recovery) or the @scala[@scaladoc[persist](akka.persistence.PersistentActor#persist[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persist](akka.persistence.AbstractPersistentActorLike#persist(A,akka.japi.Procedure))]\@scala[@scaladoc[persistAll](akka.persistence.PersistentActor#persistAll[A](events:Seq[A])(handler:A=%3EUnit):Unit)]@java[@javadoc[persistAll](akka.persistence.AbstractPersistentActorLike#persistAll(java.lang.Iterable,akka.japi.Procedure))] method persisting events. You can still use/inherit from the
@apidoc[akka.actor.Stash] interface. The internal stash cooperates with the normal stash by hooking into @apidoc[unstashAll](akka.actor.Stash) {scala="#unstashAll():Unit" java="#unstashAll()"}
making sure messages are unstashed properly to the internal stash to maintain ordering guarantees.

You should be careful to not send more messages to a persistent actor than it can keep up with, otherwise the number
of stashed messages will grow without bounds. It can be wise to protect against @javadoc[OutOfMemoryError](java.lang.OutOfMemoryError) by defining a
maximum stash capacity in the mailbox configuration:

```
akka.actor.default-mailbox.stash-capacity=10000
```

Note that the stash capacity is per actor. If you have many persistent actors, e.g. when using cluster sharding,
you may need to define a small stash capacity to ensure that the total number of stashed messages in the system
doesn't consume too much memory. Additionally, the persistent actor defines three strategies to handle failure when the
internal stash capacity is exceeded. The default overflow strategy is the @apidoc[ThrowOverflowExceptionStrategy$], which
discards the current received message and throws a @apidoc[akka.actor.StashOverflowException], causing actor restart if the default
supervision strategy is used. You can override the @apidoc[internalStashOverflowStrategy](akka.persistence.PersistenceStash) {scala="#internalStashOverflowStrategy:akka.persistence.StashOverflowStrategy" java="#internalStashOverflowStrategy()"} method to return
@apidoc[DiscardToDeadLetterStrategy$] or @apidoc[ReplyToStrategy] for any "individual" persistent actor, or define the "default"
for all persistent actors by providing FQCN, which must be a subclass of @apidoc[StashOverflowStrategyConfigurator], in the
persistence configuration:

```
akka.persistence.internal-stash-overflow-strategy=
  "akka.persistence.ThrowExceptionConfigurator"
```

The `DiscardToDeadLetterStrategy` strategy also has a pre-packaged companion configurator
@apidoc[akka.persistence.DiscardConfigurator].

You can also query the default strategy via the Akka persistence extension singleton:    

Scala
:   @@@vars
    ```
    Persistence(context.system).defaultInternalStashOverflowStrategy
    ```
    @@@

Java
:   @@@vars
    ```
    Persistence.get(getContext().getSystem()).defaultInternalStashOverflowStrategy();
    ```
    @@@

@@@ note

The bounded mailbox should be avoided in the persistent actor, by which the messages come from storage backends may
be discarded. You can use bounded stash instead of it.

@@@

<a id="persist-async"></a>
### Relaxed local consistency requirements and high throughput use-cases

If faced with relaxed local consistency requirements and high throughput demands sometimes @scala[`PersistentActor`]@java[`AbstractPersistentActor`] and its
@scala[@scaladoc[persist](akka.persistence.PersistentActor#persist[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persist](akka.persistence.AbstractPersistentActorLike#persist(A,akka.japi.Procedure))] may not be enough in terms of consuming incoming Commands at a high rate, because it has to wait until all
Events related to a given Command are processed in order to start processing the next Command. While this abstraction is
very useful for most cases, sometimes you may be faced with relaxed requirements about consistency – for example you may
want to process commands as fast as you can, assuming that the Event will eventually be persisted and handled properly in
the background, retroactively reacting to persistence failures if needed.

The @scala[@scaladoc[persistAsync](akka.persistence.PersistentActor#persistAsync[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persistAsync](akka.persistence.AbstractPersistentActorLike#persistAsync(A,akka.japi.Procedure))] method provides a tool for implementing high-throughput persistent actors. It will *not*
stash incoming Commands while the Journal is still working on persisting and/or user code is executing event callbacks.

In the below example, the event callbacks may be called "at any time", even after the next Command has been processed.
The ordering between events is still guaranteed ("evt-b-1" will be sent after "evt-a-2", which will be sent after "evt-a-1" etc.).

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #persist-async }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #persist-async }

@@@ note

In order to implement the pattern known as "*command sourcing*" call @scala[`persistAsync(cmd)(...)`]@java[`persistAsync`] right away on all incoming
messages and handle them in the callback.

@@@

@@@ warning

The callback will not be invoked if the actor is restarted (or stopped) in between the call to
`persistAsync` and the journal has confirmed the write.

@@@

<a id="defer"></a>
### Deferring actions until preceding persist handlers have executed

Sometimes when working with @scala[@scaladoc[persistAsync](akka.persistence.PersistentActor#persistAsync[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persistAsync](akka.persistence.AbstractPersistentActorLike#persistAsync(A,akka.japi.Procedure))] or @scala[@scaladoc[persist](akka.persistence.PersistentActor#persist[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persist](akka.persistence.AbstractPersistentActorLike#persist(A,akka.japi.Procedure))] you may find that it would be nice to define some actions in terms of
''happens-after the previous `persistAsync`/`persist` handlers have been invoked''. @scala[`PersistentActor`]@java[`AbstractPersistentActor`] provides utility methods
called @scala[@scaladoc[defer](akka.persistence.PersistentActor#defer[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[defer](akka.persistence.AbstractPersistentActorLike#defer(A,akka.japi.Procedure))] and @scala[@scaladoc[deferAsync](akka.persistence.PersistentActor#deferAsync[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[deferAsync](akka.persistence.AbstractPersistentActorLike#deferAsync(A,akka.japi.Procedure))], which work similarly to `persist` and `persistAsync` respectively yet do not persist the
passed in event. It is recommended to use them for *read* operations, and actions which do not have corresponding events in your
domain model.

Using those methods is very similar to the persist family of methods, yet they do **not** persist the passed in event.
It will be kept in memory and used when invoking the handler.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #defer }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #defer }

Notice that the `sender()` is **safe** to access in the handler callback, and will be pointing to the original sender
of the command for which this `defer` or `deferAsync` handler was called.

The calling side will get the responses in this (guaranteed) order:

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #defer-caller }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #defer-caller }

You can also call `defer` or `deferAsync` with `persist`.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #defer-with-persist }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #defer-with-persist }

@@@ warning

The callback will not be invoked if the actor is restarted (or stopped) in between the call to
`defer` or `deferAsync` and the journal has processed and confirmed all preceding writes.

@@@

### Nested persist calls

It is possible to call @scala[@scaladoc[persist](akka.persistence.PersistentActor#persist[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persist](akka.persistence.AbstractPersistentActorLike#persist(A,akka.japi.Procedure))] and @scala[@scaladoc[persistAsync](akka.persistence.PersistentActor#persistAsync[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persistAsync](akka.persistence.AbstractPersistentActorLike#persistAsync(A,akka.japi.Procedure))] inside their respective callback blocks and they will properly
retain both the thread safety (including the right value of @scala[`sender()`]@java[`getSender()`]) as well as stashing guarantees.

In general it is encouraged to create command handlers which do not need to resort to nested event persisting,
however there are situations where it may be useful. It is important to understand the ordering of callback execution in
those situations, as well as their implication on the stashing behavior (that `persist()` enforces). In the following
example two persist calls are issued, and each of them issues another persist inside its callback:

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #nested-persist-persist }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #nested-persist-persist }

When sending two commands to this @scala[`PersistentActor`]@java[`AbstractPersistentActor`], the persist handlers will be executed in the following order:

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #nested-persist-persist-caller }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #nested-persist-persist-caller }

First the "outer layer" of persist calls is issued and their callbacks are applied. After these have successfully completed,
the inner callbacks will be invoked (once the events they are persisting have been confirmed to be persisted by the journal).
Only after all these handlers have been successfully invoked will the next command be delivered to the persistent Actor.
In other words, the stashing of incoming commands that is guaranteed by initially calling `persist()` on the outer layer
is extended until all nested `persist` callbacks have been handled.

It is also possible to nest `persistAsync` calls, using the same pattern:

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #nested-persistAsync-persistAsync }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #nested-persistAsync-persistAsync }

In this case no stashing is happening, yet events are still persisted and callbacks are executed in the expected order:

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #nested-persistAsync-persistAsync-caller }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #nested-persistAsync-persistAsync-caller }

While it is possible to nest mixed `persist` and `persistAsync` with keeping their respective semantics
it is not a recommended practice, as it may lead to overly complex nesting.

@@@ warning

While it is possible to nest `persist` calls within one another,
it is *not* legal call `persist` from any other Thread than the Actors message processing Thread.
For example, it is not legal to call `persist` from Futures! Doing so will break the guarantees
that the persist methods aim to provide. Always call `persist` and `persistAsync` from within
the Actor's receive block (or methods synchronously invoked from there).

@@@

### Failures

If persistence of an event fails, `onPersistFailure` will be invoked (logging the error by default),
and the actor will unconditionally be stopped.

The reason that it cannot resume when persist fails is that it is unknown if the event was actually
persisted or not, and therefore it is in an inconsistent state. Restarting on persistent failures
will most likely fail anyway since the journal is probably unavailable. It is better to stop the
actor and after a back-off timeout start it again. The @apidoc[akka.pattern.BackoffSupervisor$] actor
is provided to support such restarts.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #backoff }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #backoff }

See @ref:[Backoff Supervision strategies](fault-tolerance.md#supervision-strategies) for more details about actor supervision.

If persistence of an event is rejected before it is stored, e.g. due to serialization error,
`onPersistRejected` will be invoked (logging a warning by default), and the actor continues with
next message.

If there is a problem with recovering the state of the actor from the journal when the actor is
started, `onRecoveryFailure` is called (logging the error by default), and the actor will be stopped.
Note that failure to load snapshot is also treated like this, but you can disable loading of snapshots
if you for example know that serialization format has changed in an incompatible way, see @ref:[Recovery customization](#recovery-custom).

### Atomic writes

Each event is stored atomically, but it is also possible to store several events atomically by
using the @scala[@scaladoc[persistAll](akka.persistence.PersistentActor#persistAll[A](events:Seq[A])(handler:A=%3EUnit):Unit)]@java[@javadoc[persistAll](akka.persistence.AbstractPersistentActorLike#persistAll(java.lang.Iterable,akka.japi.Procedure))] or @scala[@scaladoc[persistAllAsync](akka.persistence.PersistentActor#persistAllAsync[A](events:Seq[A])(handler:A=%3EUnit):Unit)]@java[@javadoc[persistAllAsync](akka.persistence.AbstractPersistentActorLike#persistAllAsync(java.lang.Iterable,akka.japi.Procedure))] method. That means that all events passed to that method
are stored or none of them are stored if there is an error.

The recovery of a persistent actor will therefore never be done partially with only a subset of events persisted by
*persistAll*.

Some journals may not support atomic writes of several events and they will then reject the `persistAll`
command, i.e. `onPersistRejected` is called with an exception (typically @javadoc[UnsupportedOperationException](java.lang.UnsupportedOperationException)).

### Batch writes

In order to optimize throughput when using @scala[@scaladoc[persistAsync](akka.persistence.PersistentActor#persistAsync[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persistAsync](akka.persistence.AbstractPersistentActorLike#persistAsync(A,akka.japi.Procedure))], a persistent actor
internally batches events to be stored under high load before writing them to
the journal (as a single batch). The batch size is dynamically determined by
how many events are emitted during the time of a journal round-trip: after
sending a batch to the journal no further batch can be sent before confirmation
has been received that the previous batch has been written. Batch writes are never
timer-based which keeps latencies at a minimum.

### Message deletion

It is possible to delete all messages (journaled by a single persistent actor) up to a specified sequence number;
Persistent actors may call the @scala[@scaladoc[deleteMessages](akka.persistence.PersistentActor#deleteMessages(toSequenceNr:Long):Unit)]@java[@javadoc[deleteMessages](akka.persistence.AbstractPersistentActorLike#deleteMessages(long))] method to this end.

Deleting messages in Event Sourcing based applications is typically either not used at all, or used in conjunction with
[snapshotting](#snapshots), i.e. after a snapshot has been successfully stored, a `deleteMessages(toSequenceNr)`
up until the sequence number of the data held by that snapshot can be issued to safely delete the previous events
while still having access to the accumulated state during replays - by loading the snapshot.

@@@ warning

If you are using @ref:[Persistence Query](persistence-query.md), query results may be missing deleted messages in a journal,
depending on how deletions are implemented in the journal plugin.
Unless you use a plugin which still shows deleted messages in persistence query results,
you have to design your application so that it is not affected by missing messages.

@@@

The result of the `deleteMessages` request is signaled to the persistent actor with a @apidoc[akka.persistence.DeleteMessagesSuccess]
message if the delete was successful or a @apidoc[akka.persistence.DeleteMessagesFailure] message if it failed.

Message deletion doesn't affect the highest sequence number of the journal, even if all messages were deleted from it after `deleteMessages` invocation.

### Persistence status handling

Persisting, deleting, and replaying messages can either succeed or fail.

|**Method**                 | **Success**            |
|---------------------------|------------------------|
|`persist` / `persistAsync` | persist handler invoked|
|`onPersistRejected`        | No automatic actions.  |
|`recovery`                 | `RecoveryCompleted`    |
|`deleteMessages`           | `DeleteMessagesSuccess`|

The most important operations (`persist` and `recovery`) have failure handlers modelled as explicit callbacks which
the user can override in the `PersistentActor`. The default implementations of these handlers emit a log message
(`error` for persist/recovery failures, and `warning` for others), logging the failure cause and information about
which message caused the failure.

For critical failures, such as recovery or persisting events failing, the persistent actor will be stopped after the failure
handler is invoked. This is because if the underlying journal implementation is signalling persistence failures it is most
likely either failing completely or overloaded and restarting right-away and trying to persist the event again will most
likely not help the journal recover – as it would likely cause a [Thundering herd problem](https://en.wikipedia.org/wiki/Thundering_herd_problem), as many persistent actors
would restart and try to persist their events again. Instead, using a @apidoc[BackoffSupervisor](akka.pattern.BackoffSupervisor$) (as described in @ref:[Failures](#failures)) which
implements an exponential-backoff strategy which allows for more breathing room for the journal to recover between
restarts of the persistent actor.

@@@ note

Journal implementations may choose to implement a retry mechanism, e.g. such that only after a write fails N number
of times a persistence failure is signalled back to the user. In other words, once a journal returns a failure,
it is considered *fatal* by Akka Persistence, and the persistent actor which caused the failure will be stopped.

Check the documentation of the journal implementation you are using for details if/how it is using this technique.

@@@

<a id="safe-shutdown"></a>
### Safely shutting down persistent actors

Special care should be given when shutting down persistent actors from the outside.
With normal Actors it is often acceptable to use the special @ref:[PoisonPill](actors.md#poison-pill) message
to signal to an Actor that it should stop itself once it receives this message – in fact this message is handled
automatically by Akka, leaving the target actor no way to refuse stopping itself when given a poison pill.

This can be dangerous when used with `PersistentActor` due to the fact that incoming commands are *stashed* while
the persistent actor is awaiting confirmation from the Journal that events have been written when @scala[@scaladoc[persist()](akka.persistence.PersistentActor#persist[A](event:A)(handler:A=%3EUnit):Unit)]@java[@javadoc[persist()](akka.persistence.AbstractPersistentActorLike#persist(A,akka.japi.Procedure))] was used.
Since the incoming commands will be drained from the Actor's mailbox and put into its internal stash while awaiting the
confirmation (thus, before calling the persist handlers) the Actor **may receive and (auto)handle the PoisonPill
before it processes the other messages which have been put into its stash**, causing a pre-mature shutdown of the Actor.

@@@ warning

Consider using explicit shut-down messages instead of `PoisonPill` when working with persistent actors.

@@@

The example below highlights how messages arrive in the Actor's mailbox and how they interact with its internal stashing
mechanism when `persist()` is used. Notice the early stop behavior that occurs when `PoisonPill` is used:

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #safe-shutdown }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #safe-shutdown }


Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #safe-shutdown-example-bad }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #safe-shutdown-example-bad }


Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #safe-shutdown-example-good }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #safe-shutdown-example-good }

### Replay Filter

See @ref:[Replay filter](typed/persistence.md#replay-filter) in the documentation of the new API.

## Snapshots

As you model your domain using actors, you may notice that some actors may be prone to accumulating extremely long event logs and experiencing long recovery times. Sometimes, the right approach may be to split out into a set of shorter lived actors. However, when this is not an option, you can use snapshots to reduce recovery times drastically.

Persistent actors can save snapshots of internal state by calling the  @apidoc[saveSnapshot](akka.persistence.Snapshotter) {scala="#saveSnapshot(snapshot:Any):Unit" java="#saveSnapshot(java.lang.Object)"} method. If saving of a snapshot
succeeds, the persistent actor receives a @apidoc[akka.persistence.SaveSnapshotSuccess] message, otherwise a @apidoc[akka.persistence.SaveSnapshotFailure] message

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #save-snapshot }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #save-snapshot }

where `metadata` is of type @apidoc[akka.persistence.SnapshotMetadata] and contains:

* persistenceId 
* sequenceNr
* timestamp

During recovery, the persistent actor is offered the latest saved snapshot via a @apidoc[akka.persistence.SnapshotOffer] message from
which it can initialize internal state.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #snapshot-offer }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #snapshot-offer }

The replayed messages that follow the `SnapshotOffer` message, if any, are younger than the offered snapshot.
They finally recover the persistent actor to its current (i.e. latest) state.

In general, a persistent actor is only offered a snapshot if that persistent actor has previously saved one or more snapshots
and at least one of these snapshots matches the @apidoc[akka.persistence.SnapshotSelectionCriteria] that can be specified for recovery.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #snapshot-criteria }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #snapshot-criteria }

If not specified, they default to @scala[@scaladoc[SnapshotSelectionCriteria.Latest](akka.persistence.SnapshotSelectionCriteria$#Latest:akka.persistence.SnapshotSelectionCriteria)]@java[@javadoc[SnapshotSelectionCriteria.latest()](akka.persistence.SnapshotSelectionCriteria#latest())] which selects the latest (= youngest) snapshot.
To disable snapshot-based recovery, applications should use @scala[@scaladoc[SnapshotSelectionCriteria.None](akka.persistence.SnapshotSelectionCriteria$#None:akka.persistence.SnapshotSelectionCriteria)]@java[@javadoc[SnapshotSelectionCriteria.none()](akka.persistence.SnapshotSelectionCriteria#none())]. A recovery where no
saved snapshot matches the specified `SnapshotSelectionCriteria` will replay all journaled messages.

@@@ note

In order to use snapshots, a default snapshot-store (`akka.persistence.snapshot-store.plugin`) must be configured,
or the @scala[`PersistentActor`]@java[persistent actor] can pick a snapshot store explicitly by overriding @scala[`def snapshotPluginId: String`]@java[`String snapshotPluginId()`].

Because some use cases may not benefit from or need snapshots, it is perfectly valid not to not configure a snapshot store.
However, Akka will log a warning message when this situation is detected and then continue to operate until
an actor tries to store a snapshot, at which point the operation will fail (by replying with an @apidoc[akka.persistence.SaveSnapshotFailure] for example).

Note that the "persistence mode" of @ref:[Cluster Sharding](cluster-sharding.md) makes use of snapshots. If you use that mode, you'll need to define a snapshot store plugin.

@@@

### Snapshot deletion

A persistent actor can delete individual snapshots by calling the @apidoc[deleteSnapshot](akka.persistence.Snapshotter) {scala="#deleteSnapshot(sequenceNr:Long):Unit" java="#deleteSnapshot(long)"} method with the sequence number of
when the snapshot was taken.

To bulk-delete a range of snapshots matching @apidoc[akka.persistence.SnapshotSelectionCriteria],
persistent actors should use the @apidoc[deleteSnapshots](akka.persistence.Snapshotter) {scala="#deleteSnapshots(criteria:akka.persistence.SnapshotSelectionCriteria):Unit" java="#deleteSnapshots(akka.persistence.SnapshotSelectionCriteria)"} method. Depending on the journal used this might be inefficient. It is 
best practice to do specific deletes with `deleteSnapshot` or to include a `minSequenceNr` as well as a `maxSequenceNr`
for the `SnapshotSelectionCriteria`.

### Snapshot status handling

Saving or deleting snapshots can either succeed or fail – this information is reported back to the persistent actor via
status messages as illustrated in the following table.

|**Method**                                   | **Success**              | **Failure message**     |
|---------------------------------------------|--------------------------|-------------------------|
|`saveSnapshot(Any)`                          | `SaveSnapshotSuccess`    | `SaveSnapshotFailure`   |
|`deleteSnapshot(Long)`                       | `DeleteSnapshotSuccess`  | `DeleteSnapshotFailure` |
|`deleteSnapshots(SnapshotSelectionCriteria)` | `DeleteSnapshotsSuccess` | `DeleteSnapshotsFailure`|

If failure messages are left unhandled by the actor, a default warning log message will be logged for each incoming failure message.
No default action is performed on the success messages, however you're free to handle them e.g. in order to delete
an in memory representation of the snapshot, or in the case of failure to attempt save the snapshot again.

### Optional snapshots

By default, the persistent actor will unconditionally be stopped if the snapshot can't be loaded in the recovery.
It is possible to make snapshot loading optional. This can be useful when it is alright to ignore snapshot in case
of for example deserialization errors. When snapshot loading fails it will instead recover by replaying all events.

Enable this feature by setting `snapshot-is-optional = true` in the snapshot store configuration. 

@@@ warning

Don't set `snapshot-is-optional = true` if events have been deleted because that would result in wrong recovered state if snapshot load fails.

@@@

## Scaling out

See @ref:[Scaling out](typed/persistence.md#scaling-out) in the documentation of the new API.

## At-Least-Once Delivery

To send messages with at-least-once delivery semantics to destinations you can @scala[mix-in @scaladoc[AtLeastOnceDelivery](akka.persistence.AtLeastOnceDelivery) trait to your @scaladoc[PersistentActor](akka.persistence.PersistentActor)]@java[extend the @javadoc[AbstractPersistentActorWithAtLeastOnceDelivery](akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery) class instead of @javadoc[AbstractPersistentActor](akka.persistence.AbstractPersistentActor)]
on the sending side.  It takes care of re-sending messages when they
have not been confirmed within a configurable timeout.

The state of the sending actor, including which messages have been sent that have not been
confirmed by the recipient must be persistent so that it can survive a crash of the sending actor
or JVM. The @scala[`AtLeastOnceDelivery` trait]@java[`AbstractPersistentActorWithAtLeastOnceDelivery` class] does not persist anything by itself. It is your
responsibility to persist the intent that a message is sent and that a confirmation has been
received.

@@@ note

At-least-once delivery implies that original message sending order is not always preserved,
and the destination may receive duplicate messages.
Semantics do not match those of a normal @apidoc[akka.actor.ActorRef] send operation:

 * it is not at-most-once delivery
 * message order for the same sender–receiver pair is not preserved due to
possible resends
 * after a crash and restart of the destination messages are still
delivered to the new actor incarnation

These semantics are similar to what an @apidoc[akka.actor.ActorPath] represents (see
@ref:[Actor Lifecycle](actors.md#actor-lifecycle)), therefore you need to supply a path and not a
reference when delivering messages. The messages are sent to the path with
an actor selection.

@@@

Use the @scala[@scaladoc[deliver](akka.persistence.AtLeastOnceDelivery#deliver(destination:akka.actor.ActorPath)(deliveryIdToMessage:Long=%3EAny):Unit)]@java[@javadoc[deliver](akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery#deliver(akka.actor.ActorPath,akka.japi.Function))] method to send a message to a destination. Call the @scala[@scaladoc[confirmDelivery](akka.persistence.AtLeastOnceDelivery#confirmDelivery(deliveryId:Long):Boolean)]@java[@javadoc[confirmDelivery](akka.persistence.AtLeastOnceDeliveryLike#confirmDelivery(long))] method
when the destination has replied with a confirmation message.

### Relationship between deliver and confirmDelivery

To send messages to the destination path, use the `deliver` method after you have persisted the intent
to send the message.

The destination actor must send back a confirmation message. When the sending actor receives this
confirmation message you should persist the fact that the message was delivered successfully and then call
the `confirmDelivery` method.

If the persistent actor is not currently recovering, the `deliver` method will send the message to
the destination actor. When recovering, messages will be buffered until they have been confirmed using `confirmDelivery`.
Once recovery has completed, if there are outstanding messages that have not been confirmed (during the message replay),
the persistent actor will resend these before sending any other messages.

Deliver requires a `deliveryIdToMessage` function to pass the provided `deliveryId` into the message so that the correlation
between `deliver` and `confirmDelivery` is possible. The `deliveryId` must do the round trip. Upon receipt
of the message, the destination actor will send the same``deliveryId`` wrapped in a confirmation message back to the sender.
The sender will then use it to call `confirmDelivery` method to complete the delivery routine.

Scala
:  @@snip [PersistenceDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceDocSpec.scala) { #at-least-once-example }

Java
:  @@snip [LambdaPersistenceDocTest.java](/akka-docs/src/test/java/jdocs/persistence/LambdaPersistenceDocTest.java) { #at-least-once-example }

The `deliveryId` generated by the persistence module is a strictly monotonically increasing sequence number
without gaps. The same sequence is used for all destinations of the actor, i.e. when sending to multiple
destinations the destinations will see gaps in the sequence. It is not possible to use custom `deliveryId`.
However, you can send a custom correlation identifier in the message to the destination. You must then retain
a mapping between the internal `deliveryId` (passed into the `deliveryIdToMessage` function) and your custom
correlation id (passed into the message). You can do this by storing such mapping in a `Map(correlationId -> deliveryId)`
from which you can retrieve the `deliveryId` to be passed into the `confirmDelivery` method once the receiver
of your message has replied with your custom correlation id.

The @scala[@scaladoc[AtLeastOnceDelivery](akka.persistence.AtLeastOnceDelivery) trait]@java[@javadoc[AbstractPersistentActorWithAtLeastOnceDelivery](akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery) class] has a state consisting of unconfirmed messages and a
sequence number. It does not store this state itself. You must persist events corresponding to the
@scala[@scaladoc[deliver](akka.persistence.AtLeastOnceDelivery#deliver(destination:akka.actor.ActorPath)(deliveryIdToMessage:Long=%3EAny):Unit)]@java[@javadoc[deliver](akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery#deliver(akka.actor.ActorPath,akka.japi.Function))] and @scala[@scaladoc[confirmDelivery](akka.persistence.AtLeastOnceDelivery#confirmDelivery(deliveryId:Long):Boolean)]@java[@javadoc[confirmDelivery](akka.persistence.AtLeastOnceDeliveryLike#confirmDelivery(long))] invocations from your `PersistentActor` so that the state can
be restored by calling the same methods during the recovery phase of the `PersistentActor`. Sometimes
these events can be derived from other business level events, and sometimes you must create separate events.
During recovery, calls to `deliver` will not send out messages, those will be sent later
if no matching `confirmDelivery` will have been performed.

Support for snapshots is provided by @apidoc[getDeliverySnapshot](akka.persistence.AtLeastOnceDeliveryLike) {scala="#getDeliverySnapshot:akka.persistence.AtLeastOnceDelivery.AtLeastOnceDeliverySnapshot" java="#getDeliverySnapshot()"} and @apidoc[setDeliverySnapshot](akka.persistence.AtLeastOnceDeliveryLike) {scala="#setDeliverySnapshot(snapshot:akka.persistence.AtLeastOnceDelivery.AtLeastOnceDeliverySnapshot):Unit" java="#setDeliverySnapshot(akka.persistence.AtLeastOnceDelivery.AtLeastOnceDeliverySnapshot)"}.
The @apidoc[akka.persistence.AtLeastOnceDelivery.AtLeastOnceDeliverySnapshot] contains the full delivery state, including unconfirmed messages.
If you need a custom snapshot for other parts of the actor state you must also include the
`AtLeastOnceDeliverySnapshot`. It is serialized using protobuf with the ordinary Akka
serialization mechanism. It is easiest to include the bytes of the `AtLeastOnceDeliverySnapshot`
as a blob in your custom snapshot.

The interval between redelivery attempts is defined by the @apidoc[redeliverInterval](akka.persistence.AtLeastOnceDeliveryLike) {scala="#redeliverInterval:scala.concurrent.duration.FiniteDuration" java="#redeliverInterval()"} method.
The default value can be configured with the `akka.persistence.at-least-once-delivery.redeliver-interval`
configuration key. The method can be overridden by implementation classes to return non-default values.

The maximum number of messages that will be sent at each redelivery burst is defined by the
@apidoc[redeliverBurstLimit](akka.persistence.AtLeastOnceDeliveryLike) {scala="#redeliveryBurstLimit:Int" java="#redeliveryBurstLimit()"} method (burst frequency is half of the redelivery interval). If there's a lot of
unconfirmed messages (e.g. if the destination is not available for a long time), this helps to prevent an overwhelming
amount of messages to be sent at once. The default value can be configured with the
`akka.persistence.at-least-once-delivery.redelivery-burst-limit` configuration key. The method can be overridden
by implementation classes to return non-default values.

After a number of delivery attempts a @apidoc[akka.persistence.AtLeastOnceDelivery.UnconfirmedWarning] message
will be sent to `self`. The re-sending will still continue, but you can choose to call
`confirmDelivery` to cancel the re-sending. The number of delivery attempts before emitting the
warning is defined by the @apidoc[warnAfterNumberOfUnconfirmedAttempts](akka.persistence.AtLeastOnceDeliveryLike) {scala="#warnAfterNumberOfUnconfirmedAttempts:Int" java="#warnAfterNumberOfUnconfirmedAttempts()"} method. The default value can be
configured with the `akka.persistence.at-least-once-delivery.warn-after-number-of-unconfirmed-attempts`
configuration key. The method can be overridden by implementation classes to return non-default values.

The @scala[@scaladoc[AtLeastOnceDelivery](akka.persistence.AtLeastOnceDelivery) trait]@java[@javadoc[AbstractPersistentActorWithAtLeastOnceDelivery](akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery) class] holds messages in memory until their successful delivery has been confirmed.
The maximum number of unconfirmed messages that the actor is allowed to hold in memory
is defined by the @apidoc[maxUnconfirmedMessages](akka.persistence.AtLeastOnceDeliveryLike) {scala="#maxUnconfirmedMessages:Int" java="#maxUnconfirmedMessages()"} method. If this limit is exceed the `deliver` method will
not accept more messages and it will throw @apidoc[AtLeastOnceDelivery.MaxUnconfirmedMessagesExceededException].
The default value can be configured with the `akka.persistence.at-least-once-delivery.max-unconfirmed-messages`
configuration key. The method can be overridden by implementation classes to return non-default values.

## Event Adapters

In long running projects using Event Sourcing sometimes the need arises to detach the data model from the domain model
completely.

Event Adapters help in situations where:

 * **Version Migrations** – existing events stored in *Version 1* should be "upcasted" to a new *Version 2* representation,
and the process of doing so involves actual code, not just changes on the serialization layer. For these scenarios
the @apidoc[toJournal](akka.persistence.journal.WriteEventAdapter) {scala="#toJournal(event:Any):Any" java="#toJournal(java.lang.Object)"} function is usually an identity function, however the @apidoc[fromJournal](akka.persistence.journal.ReadEventAdapter) {scala="#fromJournal(event:Any,manifest:String):akka.persistence.journal.EventSeq" java="#fromJournal(java.lang.Object,java.lang.String)"} is implemented as
`v1.Event=>v2.Event`, performing the necessary mapping inside the fromJournal method.
This technique is sometimes referred to as "upcasting" in other CQRS libraries.
 * **Separating Domain and Data models** – thanks to EventAdapters it is possible to completely separate the domain model
from the model used to persist data in the Journals. For example one may want to use case classes in the
domain model, however persist their protocol-buffer (or any other binary serialization format) counter-parts to the Journal.
A simple `toJournal:MyModel=>MyDataModel` and `fromJournal:MyDataModel=>MyModel` adapter can be used to implement this feature.
 * **Journal Specialized Data Types** – exposing data types understood by the underlying Journal, for example for data stores which
understand JSON it is possible to write an EventAdapter `toJournal:Any=>JSON` such that the Journal can *directly* store the
json instead of serializing the object to its binary representation.

Implementing an EventAdapter is rather straightforward:

Scala
:  @@snip [PersistenceEventAdapterDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceEventAdapterDocSpec.scala) { #identity-event-adapter }

Java
:  @@snip [PersistenceEventAdapterDocTest.java](/akka-docs/src/test/java/jdocs/persistence/PersistenceEventAdapterDocTest.java) { #identity-event-adapter }

Then in order for it to be used on events coming to and from the journal you must bind it using the below configuration syntax:

@@snip [PersistenceEventAdapterDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceEventAdapterDocSpec.scala) { #event-adapters-config }

It is possible to bind multiple adapters to one class *for recovery*, in which case the `fromJournal` methods of all
bound adapters will be applied to a given matching event (in order of definition in the configuration). Since each adapter may
return from `0` to `n` adapted events (called as `EventSeq`), each adapter can investigate the event and if it should
indeed adapt it return the adapted event(s) for it. Other adapters which do not have anything to contribute during this
adaptation simply return @apidoc[EventSeq.empty](akka.persistence.journal.EventSeq$) {scala="#empty:akka.persistence.journal.EventSeq" java="#empty()"}. The adapted events are then delivered in-order to the `PersistentActor` during replay.

@@@ note

For more advanced schema evolution techniques refer to the @ref:[Persistence - Schema Evolution](persistence-schema-evolution.md) documentation.

@@@

## Custom serialization

Serialization of snapshots and payloads of `Persistent` messages is configurable with Akka's
@ref:[Serialization](serialization.md) infrastructure. For example, if an application wants to serialize

 * payloads of type `MyPayload` with a custom `MyPayloadSerializer` and
 * snapshots of type `MySnapshot` with a custom `MySnapshotSerializer`

it must add

@@snip [PersistenceSerializerDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceSerializerDocSpec.scala) { #custom-serializer-config }

to the application configuration. If not specified, an exception will be throw when trying to persist events or snapshots.

For more advanced schema evolution techniques refer to the @ref:[Persistence - Schema Evolution](persistence-schema-evolution.md) documentation.

## Testing with LevelDB journal

The LevelDB journal is deprecated and will be removed from a future Akka version, it is not advised to build new applications 
with it. For testing the built in "inmem" journal or the actual journal that will be used in production of the application 
is recommended. See @ref[Persistence Plugins](persistence-plugins.md) for some journal implementation choices.

When running tests with LevelDB default settings in `sbt`, make sure to set `fork := true` in your sbt project. Otherwise, you'll see an @javadoc[UnsatisfiedLinkError](java.lang.UnsatisfiedLinkError). Alternatively, you can switch to a LevelDB Java port by setting

@@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #native-config }

or

@@snip [PersistencePluginDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistencePluginDocSpec.scala) { #shared-store-native-config }

in your Akka configuration. Also note that for the LevelDB Java port, you will need the following dependencies:

@@dependency[sbt,Maven,Gradle] {
  group="org.iq80.leveldb"
  artifact="leveldb"
  version="0.9"
}

@@@ note { title="Java 17" }

When using LevelDB with Java 17 you have to add JVM flag `--add-opens=java.base/java.nio=ALL-UNNAMED`. 

@@@

@@@ warning

It is not possible to test persistence provided classes (i.e. `PersistentActor`
and @ref:[AtLeastOnceDelivery](#at-least-once-delivery)) using @apidoc[TestActorRef] due to its *synchronous* nature.
These traits need to be able to perform asynchronous tasks in the background in order to handle internal persistence
related events.

When testing Persistence based projects always rely on @ref:[asynchronous messaging using the TestKit](testing.md#async-integration-testing).

@@@

## Configuration

There are several configuration properties for the persistence module, please refer
to the @ref:[reference configuration](general/configuration-reference.md#config-akka-persistence).

The @ref:[journal and snapshot store plugins](persistence-plugins.md) have specific configuration, see
reference documentation of the chosen plugin.

## Multiple persistence plugin configurations

By default, a persistent actor will use the "default" journal and snapshot store plugins
configured in the following sections of the `reference.conf` configuration resource:

@@snip [PersistenceMultiDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceMultiDocSpec.scala) { #default-config }

Note that in this case the actor overrides only the @apidoc[persistenceId](akka.persistence.PersistenceIdentity) {scala="#persistenceId:String" java="#persistenceId()"} method:

Scala
:  @@snip [PersistenceMultiDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceMultiDocSpec.scala) { #default-plugins }

Java
:  @@snip [PersistenceMultiDocTest.java](/akka-docs/src/test/java/jdocs/persistence/PersistenceMultiDocTest.java) { #default-plugins }

When the persistent actor overrides the @apidoc[journalPluginId](akka.persistence.PersistenceIdentity) {scala="#journalPluginId:String" java="#journalPluginId()"} and @apidoc[snapshotPluginId](akka.persistence.PersistenceIdentity) {scala="#snapshotPluginId:String" java="#snapshotPluginId()"} methods,
the actor will be serviced by these specific persistence plugins instead of the defaults:

Scala
:  @@snip [PersistenceMultiDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceMultiDocSpec.scala) { #override-plugins }

Java
:  @@snip [PersistenceMultiDocTest.java](/akka-docs/src/test/java/jdocs/persistence/PersistenceMultiDocTest.java) { #override-plugins }

Note that `journalPluginId` and `snapshotPluginId` must refer to properly configured `reference.conf`
plugin entries with a standard `class` property as well as settings which are specific for those plugins, i.e.:

@@snip [PersistenceMultiDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceMultiDocSpec.scala) { #override-config }

## Give persistence plugin configurations at runtime

By default, a persistent actor will use the configuration loaded at @apidoc[akka.actor.ActorSystem] creation time to create journal and snapshot store plugins.

When the persistent actor overrides the @apidoc[journalPluginConfig](akka.persistence.RuntimePluginConfig) {scala="#journalPluginConfig:com.typesafe.config.Config" java="#journalPluginConfig()"} and @apidoc[snapshotPluginConfig](akka.persistence.RuntimePluginConfig) {scala="#snapshotPluginConfig:com.typesafe.config.Config" java="#snapshotPluginConfig()"} methods,
the actor will use the declared @javadoc[Config](com.typesafe.config.Config) objects with a fallback on the default configuration.
It allows a dynamic configuration of the journal and the snapshot store at runtime:

Scala
:  @@snip [PersistenceMultiDocSpec.scala](/akka-docs/src/test/scala/docs/persistence/PersistenceMultiDocSpec.scala) { #runtime-config }

Java
:  @@snip [PersistenceMultiDocTest.java](/akka-docs/src/test/java/jdocs/persistence/PersistenceMultiDocTest.java) { #runtime-config }

## See also

* @ref[Persistence plugins](persistence-plugins.md)
* @ref[Building a new storage backend](persistence-journals.md)
