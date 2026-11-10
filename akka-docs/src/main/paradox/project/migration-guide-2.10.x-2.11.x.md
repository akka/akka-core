---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.10.0 or earlier have been removed. The sections below list the removed APIs and their replacements.

### akka-actor-testkit-typed

* `LoggingTestKit.intercept` (scaladsl): use `expect` instead.

### akka-actor-typed

* `akka.actor.typed.scaladsl.LoggerOps`: not needed in Scala 2.13 and later, use the `org.slf4j.Logger` methods directly.
* `TimerScheduler.startPeriodicTimer` (scaladsl and javadsl): use `startTimerWithFixedDelay` or `startTimerAtFixedRate` instead. In scaladsl, `startPeriodicTimer` had the same semantics as `startTimerAtFixedRate`. In javadsl, it had the same semantics as `startTimerWithFixedDelay`, which is often preferred.

### akka-cluster

* `UniqueAddress` constructor and `UniqueAddress.apply` with an `Int` uid, `UniqueAddress.uid` and `UniqueAddress.copy`: use the `Long` uid constructor or `apply`, and `longUid`.
* `ClusterEvent.CurrentClusterState` constructors without `unreachableDataCenters` and `memberTombstones`: use the main constructor, or `CurrentClusterState.apply`, which keeps the default arguments.

### akka-cluster-sharding-typed

* The deprecated `ClusterShardingSettings` constructors: use `ClusterShardingSettings(system)` or `ClusterShardingSettings.create(system)` with the `with*` methods, or the full constructor.
* `ClusterShardingSettings.passivateIdleEntityAfter` and `withPassivateIdleEntityAfter`: use `withPassivationStrategy` and `passivationStrategySettings`, see @ref:[Automatic Passivation](../typed/cluster-sharding.md).
* `ShardingProducerController.Settings.resendFirsUnconfirmedIdleTimeout` and `withResendFirsUnconfirmedIdleTimeout`: use the correctly spelled `resendFirstUnconfirmedIdleTimeout` and `withResendFirstUnconfirmedIdleTimeout`.

### akka-cluster-tools

* `DistributedPubSubSettings` constructor without `sendToDeadLettersWhenNoSubscribers`: use the constructor with `sendToDeadLettersWhenNoSubscribers`, or `DistributedPubSubSettings(config)`.

### akka-cluster-typed

* `ClusterSingletonSettings` and `ClusterSingletonManagerSettings` constructors without `leaseSettings`: use the constructor with `leaseSettings`.

### akka-distributed-data

* `ReplicatorSettings` constructors with fewer parameters than the full constructor: use the full constructor, `ReplicatorSettings(config)` or the `with*` methods.
* `ORMap.updated(Cluster, key, initial, modify)` (Java API): use `ORMap.update(SelfUniqueAddress, key, initial, modify)`.
* `GSet.copy$default$1`, a binary compatibility stub in the Scala 2 sources only.

### akka-multi-node-testkit

* `TestConductor.throttle`: throttle was not implemented, use `blackhole` and `passThrough` instead.

### akka-persistence

* The LevelDB journal (`akka.persistence.journal.leveldb`, `LeveldbJournal`) and the shared LevelDB journal (`akka.persistence.journal.leveldb-shared`, `SharedLeveldbJournal`, `SharedLeveldbStore`): use another journal implementation, such as [Akka Persistence R2DBC](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/). For tests, use the `akka.persistence.journal.inmem` journal, in combination with the @ref:[journal proxy](../persistence-plugins.md#persistence-plugin-proxy) when several actor systems share one journal. The `akka.persistence.journal.leveldb` and `akka.persistence.journal.leveldb-shared` configuration sections no longer exist.
* `DurableStateUpdateStore.deleteObject(persistenceId)` in `akka.persistence.state.scaladsl` and `akka.persistence.state.javadsl`: use the `deleteObject` overload with `revision` instead. Durable state store plugins must remove their implementation of the overload without `revision`.

### akka-persistence-query

* The LevelDB read journal (`akka.persistence.query.journal.leveldb`, `LeveldbReadJournal` in scaladsl and javadsl, `LeveldbReadJournalProvider`): use another journal and query implementation, such as [Akka Persistence R2DBC](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/). The `akka.persistence.query.journal.leveldb` configuration section no longer exists.
* `EventEnvelope(offset, persistenceId, sequenceNr, event)` and `new EventEnvelope(offset, persistenceId, sequenceNr, event)` in `akka.persistence.query`: use the overloads with a `timestamp` parameter. The `EventEnvelope` companion object no longer extends `Function4`.

### akka-persistence-typed

* `snapshotSelectionCriteria` in the javadsl `EventSourcedBehavior`, `EventSourcedOnCommandBehavior` and `EventSourcedOnCommandWithReplyBehavior`: override `recovery` instead and return `Recovery.withSnapshotSelectionCriteria(...)`. Overriding `snapshotSelectionCriteria` had no effect since Akka 2.6.5.
* `EventSourcedBehavior.withSnapshotSelectionCriteria` in scaladsl: use `withRecovery(Recovery.withSnapshotSelectionCriteria(...))`.

### akka-stream-testkit

* `TestSink.probe` and `TestSource.probe` (scaladsl): use `TestSink()` and `TestSource()` with an implicit `ClassicActorSystemProvider` instead.
* `TestSink.probe(system)` and `TestSource.probe(system)` (javadsl): use `TestSink.create(system)` and `TestSource.create(system)` with a `ClassicActorSystemProvider` instead.

### akka-stream-typed

* `ActorSource.actorRefWithAck` (scaladsl and javadsl): use `ActorSource.actorRefWithBackpressure` instead.

### akka-testkit

* `akka.testkit.javadsl.TestKit` overloads of `expectMsgPF`, `expectTerminated`, `fishForMessage` and `fishForSpecificMessage` taking a `scala.concurrent.duration.Duration`: use the overloads that accept `java.time.Duration` instead.
