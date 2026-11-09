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

### akka-cluster-sharding-typed

* The deprecated `ClusterShardingSettings` constructors: use `ClusterShardingSettings(system)` or `ClusterShardingSettings.create(system)` with the `with*` methods, or the full constructor.
* `ClusterShardingSettings.passivateIdleEntityAfter` and `withPassivateIdleEntityAfter`: use `withPassivationStrategy` and `passivationStrategySettings`, see @ref:[Automatic Passivation](../typed/cluster-sharding.md).
* `ShardingProducerController.Settings.resendFirsUnconfirmedIdleTimeout` and `withResendFirsUnconfirmedIdleTimeout`: use the correctly spelled `resendFirstUnconfirmedIdleTimeout` and `withResendFirstUnconfirmedIdleTimeout`.

### akka-cluster

* `UniqueAddress` constructor and `UniqueAddress.apply` with an `Int` uid, `UniqueAddress.uid` and `UniqueAddress.copy`: use the `Long` uid constructor or `apply`, and `longUid`.
* `ClusterEvent.CurrentClusterState` constructors without `unreachableDataCenters` and `memberTombstones`: use the main constructor, or `CurrentClusterState.apply`, which keeps the default arguments.

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

### akka-stream-typed

* `ActorSource.actorRefWithAck` (scaladsl and javadsl): use `ActorSource.actorRefWithBackpressure` instead.

### akka-testkit

* `akka.testkit.javadsl.TestKit` overloads of `expectMsgPF`, `expectTerminated`, `fishForMessage` and `fishForSpecificMessage` taking a `scala.concurrent.duration.Duration`: use the overloads that accept `java.time.Duration` instead.
