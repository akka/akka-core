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

### akka-cluster-sharding

* The deprecated `ClusterShardingSettings` and `ClusterShardingSettings.TuningParameters` constructors: use `ClusterShardingSettings(system)` or `ClusterShardingSettings.create(system)` with the `with*` methods, or the full constructor.
* `ClusterShardingSettings.passivateIdleEntityAfter` and `withPassivateIdleAfter`: use `withPassivationStrategy` and `passivationStrategySettings`, see @ref:[Automatic Passivation](../typed/cluster-sharding.md).
* `ClusterShardingHealthCheckSettings(names, timeout)`: use the full constructor.

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

### akka-remote

* `AddressUidExtension`: use `ExtendedActorSystem.uid` instead. In general `akka.cluster.UniqueAddress` is a better source for getting the uid. With typed Cluster that is accessible via `selfMember` and with classic Cluster via `selfUniqueAddress`.
* `akka.remote.serialization.LongSerializer`, `IntSerializer`, `StringSerializer` and `ByteStringSerializer`: moved to `akka.serialization` in akka-actor. The serializer ids 18 to 21 are unchanged and used by the akka-actor serializers, so persisted data still deserializes.

### akka-stream

* `ActorMaterializer`: `Materializer` has all the methods `ActorMaterializer` had. Use the system materializer: `Materializer.matFromSystem` provides it implicitly when an implicit `ActorSystem`, or another `ClassicActorSystemProvider`, is in scope. Replace `ActorMaterializer(...)(context)` in an actor with `Materializer(context)` or `Materializer.createMaterializer(context)`, and `ActorMaterializer(...)(system)` with `Materializer(system)`, to create a separate materializer.
* The `ActorMaterializerSettings`, `IOSettings` and `StreamRefSettings` factories and `with*` methods, and `Materializer.settings`: configure streams with the `akka.stream.materializer` config, or per stream with `ActorAttributes`, `TcpAttributes` and `StreamRefAttributes`. Operators read the settings from their attributes.
* The settings accessors are no longer public. Read the attribute that replaces each: `ActorMaterializerSettings.dispatcher`: `ActorAttributes.dispatcher`; `initialInputBufferSize` and `maxInputBufferSize`: `Attributes.inputBuffer`; `supervisionDecider`: `ActorAttributes.supervisionStrategy`; `debugLogging`: `ActorAttributes.debugLogging`; `outputBurstLimit`: `ActorAttributes.outputBurstLimit`; `fuzzingMode`: `ActorAttributes.fuzzingMode`; `maxFixedBufferSize`: `ActorAttributes.maxFixedBufferSize`; `syncProcessingLimit`: `ActorAttributes.syncProcessingLimit`; `blockingIoDispatcher`: the `ActorAttributes.IODispatcher` attribute, which selects the `akka.stream.materializer.blocking-io-dispatcher` config; `autoFusing`: no replacement, it had no effect. `IOSettings.tcpWriteBufferSize`: `TcpAttributes.tcpWriteBufferSize`. `StreamSubscriptionTimeoutSettings.mode` and `timeout`: `ActorAttributes.streamSubscriptionTimeout`. `StreamRefSettings.bufferCapacity`, `demandRedeliveryInterval`, `subscriptionTimeout` and `finalTerminationSignalDeadline`: the `StreamRefAttributes` methods with the same names.
* `IOResult.status`, `wasSuccessful`, `getError` and `createFailed`, and `AbruptIOTerminationException`: IO operators fail the materialized value with `IOOperationIncompleteException`. `FileIO.fromPath` now also fails the materialized value when the file can't be read, instead of completing it with a failed status.
* The `copy`, `unapply` and `Product` methods of `Attributes`: use the explicit methods on `Attributes`. `Attributes` no longer extends `Product`.
* `Source.fromFuture`, `fromCompletionStage`, `fromFutureSource` and `fromSourceCompletionStage`: use `Source.future`, `Source.completionStage`, `Source.futureSource` and `Source.completionStageSource`.
* `Source.lazily` and `Source.lazilyAsync`: use `Source.lazySource`, `Source.lazyFuture` or `Source.lazyCompletionStage`.
* `Sink.lazyInit` and `Sink.lazyInitAsync`: use `Sink.lazyFutureSink` or `Sink.lazyCompletionStageSink`. `Flow.lazyInit` and `Flow.lazyInitAsync`: use `Flow.lazyFutureFlow` or `Flow.lazyCompletionStageFlow`.
* `Source.setup`, `Flow.setup` and `Sink.setup`: use `fromMaterializer`.
* `Source.actorRef(bufferSize, overflowStrategy)` and `Sink.actorRef(ref, onCompleteMessage)`: use the overloads that take completion and failure matchers, or an on-complete and an on-failure message. `Source.actorRefWithAck` and `Sink.actorRefWithAck`: use `actorRefWithBackpressure`.
* `FileIO.fromFile` and `FileIO.toFile`: use `FileIO.fromPath` and `FileIO.toPath`.
* The `akka.stream.javadsl.Tcp.bind` and `outgoingConnection` overloads that take a `scala.concurrent.duration.Duration`: use the overloads that take a `java.time.Duration`.
* The `RestartSource`, `RestartFlow` and `RestartSink` overloads without `RestartSettings`: use the overloads that take `RestartSettings`.
* `TimerGraphStageLogic.schedulePeriodically`, `schedulePeriodicallyWithInitialDelay` and `Materializer.schedulePeriodically`: use `scheduleWithFixedDelay` or `scheduleAtFixedRate`.
* The `BindFailedException` object: match on the `BindFailedException` class instead.

### akka-stream-testkit

* `TestSink.probe` and `TestSource.probe` (scaladsl): use `TestSink()` and `TestSource()` with an implicit `ClassicActorSystemProvider` instead.
* `TestSink.probe(system)` and `TestSource.probe(system)` (javadsl): use `TestSink.create(system)` and `TestSource.create(system)` with a `ClassicActorSystemProvider` instead.

### akka-stream-typed

* `ActorSource.actorRefWithAck` (scaladsl and javadsl): use `ActorSource.actorRefWithBackpressure` instead.

### akka-testkit

* `akka.testkit.javadsl.TestKit` overloads of `expectMsgPF`, `expectTerminated`, `fishForMessage` and `fishForSpecificMessage` taking a `scala.concurrent.duration.Duration`: use the overloads that accept `java.time.Duration` instead.
