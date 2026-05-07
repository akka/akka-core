---
project.description: Append only event logs, snapshots and recovery with Akka event sourced actors.
---
# Snapshotting

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Akka Persistence](../persistence.md).

## Snapshots

As you model your domain using @ref:[event sourced actors](persistence.md), you may notice that some actors may be
prone to accumulating extremely long event logs and experiencing long recovery times. Sometimes, the right approach
may be to split out into a set of shorter lived actors. However, when this is not an option, you can use snapshots
to reduce recovery times drastically.

Persistent actors can save snapshots of internal state every N events or when a given predicate of the state
is fulfilled.

Scala
:  @@snip [BasicPersistentActorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #retentionCriteria }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #retentionCriteria }


Scala
:  @@snip [BasicPersistentActorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #snapshottingPredicate }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #snapshottingPredicate }

When a snapshot is triggered, incoming commands are stashed until the snapshot has been saved. This means that
the state can safely be mutable although the serialization and storage of the state is performed asynchronously.
The state instance will not be updated by new events until after the snapshot has been saved.

During recovery, the persistent actor is using the latest saved snapshot to initialize the state. Thereafter the events
after the snapshot are replayed using the event handler to recover the persistent actor to its current (i.e. latest)
state.

If not specified, they default to @scala[`SnapshotSelectionCriteria.Latest`]@java[`SnapshotSelectionCriteria.latest()`]
which selects the latest (youngest) snapshot. It's possible to override the selection of which snapshot to use for
recovery like this:

Scala
:  @@snip [BasicPersistentActorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #snapshotSelection }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #snapshotSelection }

To disable snapshot-based recovery, applications can use @scala[`SnapshotSelectionCriteria.None`]@java[`SnapshotSelectionCriteria.none()`].
A recovery where no saved snapshot matches the specified `SnapshotSelectionCriteria` will replay all journaled
events. This can be useful if snapshot serialization format has changed in an incompatible way. It should typically
not be used when events have been deleted.

In order to use snapshots, a default snapshot-store (`akka.persistence.snapshot-store.plugin`) must be configured,
or you can pick a snapshot store for for a specific `EventSourcedBehavior` by
@scala[defining it with `withSnapshotPluginId` of the `EventSourcedBehavior`]@java[overriding `snapshotPluginId` in
the `EventSourcedBehavior`].

Because some use cases may not benefit from or need snapshots, it is perfectly valid not to not configure a snapshot store.
However, Akka will log a warning message when this situation is detected and then continue to operate until
an actor tries to store a snapshot, at which point the operation will fail.

## Snapshot failures

Saving snapshots can either succeed or fail – this information is reported back to the persistent actor via
the `SnapshotCompleted` or `SnapshotFailed` signal. Snapshot failures are logged by default but do not cause
the actor to stop or restart.

If there is a problem with recovering the state of the actor from the journal when the actor is
started, `RecoveryFailed` signal is emitted (logging the error by default), and the actor will be stopped.
Note that failure to load snapshot is also treated like this, but you can disable loading of snapshots
if you for example know that serialization format has changed in an incompatible way.

### Optional snapshots

By default, the persistent actor will unconditionally be stopped if the snapshot can't be loaded in the recovery.
It is possible to make snapshot loading optional. This can be useful when it is alright to ignore snapshot in case
of for example deserialization errors. When snapshot loading fails it will instead recover by replaying all events.

Enable this feature by setting `snapshot-is-optional = true` in the snapshot store configuration.

@@@ warning

Don't set `snapshot-is-optional = true` if events have been deleted because that would result in wrong recovered state if snapshot load fails.

@@@

## Snapshot deletion

To free up space, an event sourced actor can automatically delete older snapshots based on the given `RetentionCriteria`.

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #retentionCriteria }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #retentionCriteria #snapshottingPredicate }

Snapshot deletion is triggered after successfully saving a new snapshot.

The above example will save snapshots automatically every `numberOfEvents = 100`. Snapshots that have sequence
number less than the sequence number of the saved snapshot minus `keepNSnapshots * numberOfEvents` (`100 * 2`) are automatically
deleted.

In addition, it will also save a snapshot when the persisted event is `BookingCompleted`. Automatic snapshotting
based on `numberOfEvents` can be used without specifying @scala[`snapshotWhen`]@java[`shouldSnapshot`]. Snapshots
triggered by the @scala[`snapshotWhen`]@java[`shouldSnapshot`] predicate will not trigger deletion of old snapshots.

On async deletion, either a `DeleteSnapshotsCompleted` or `DeleteSnapshotsFailed` signal is emitted.
You can react to signal outcomes by using @scala[with `receiveSignal` handler] @java[by overriding `receiveSignal`].
By default, successful completion is logged by the system at log level `debug`, failures at log level `warning`.

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #retentionCriteriaWithSignals }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #retentionCriteriaWithSignals }

## Event deletion

Deleting events in Event Sourcing based applications is typically either not used at all, or used in conjunction with snapshotting.
By deleting events you will lose the history of how the system changed before it reached current state, which is
one of the main reasons for using Event Sourcing in the first place.

Immediate event deletion should not be used together with Projections and for @ref:[Replicated Event Sourcing](replicated-eventsourcing.md)
event deletion via snapshot predicate or retention criteria is not allowed. Deleting all events immediately when an
entity has reached its terminal deleted state would have the consequence that Projections might not have consumed all
previous events yet and will not be notified of the deleted event. Instead, it's recommended to emit a final deleted
event and store the fact that the entity is deleted separately via a Projection. Then a background task can clean up
the events and snapshots for the deleted entities. The entity itself knows about the terminal state from the deleted
event and should not emit further events after that and typically stop itself if it receives more commands.

If snapshots are triggered using the predicate-based api (@scala[`snapshotWhen`]@java[`shouldSnapshot`]), events are not deleted 
by default. Event deletion can be enabled using the following api:

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #snapshottingPredicateDeleteEvents }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #snapshottingPredicateDeleteEvents }

If snapshot-based retention is enabled, after a snapshot has been successfully stored, a delete of the events
(journaled by a single event sourced actor) up until the sequence number of the data held by that snapshot can be issued.

To elect to use this, enable `withDeleteEventsOnSnapshot` of the `RetentionCriteria` which is disabled by default.

Scala
:  @@snip [BasicPersistentBehaviorCompileOnly.scala](/akka-persistence-typed/src/test/scala/docs/akka/persistence/typed/BasicPersistentBehaviorCompileOnly.scala) { #snapshotAndEventDeletes }

Java
:  @@snip [BasicPersistentBehaviorTest.java](/akka-persistence-typed/src/test/java/jdocs/akka/persistence/typed/BasicPersistentBehaviorTest.java) { #snapshotAndEventDeletes }

Event deletion is triggered after saving a new snapshot. Old events would be deleted prior to old snapshots being deleted.

On async deletion, either a `DeleteEventsCompleted` or `DeleteEventsFailed` signal is emitted.
You can react to signal outcomes by using @scala[with `receiveSignal` handler] @java[by overriding `receiveSignal`].
By default, successful completion is logged by the system at log level `debug`, failures at log level `warning`.

Message deletion does not affect the highest sequence number of the journal, even if all messages were deleted from it after a delete occurs.

@@@ note

It is up to the journal implementation whether events are actually removed from storage.

@@@
