---
project.description: Migrating to Akka 2.11.
---
# Migration Guide 2.10.x to 2.11.x

## Jackson 2.21

@ref:[Akka Serialization Jackson](../serialization-jackson.md) now depends on the Jackson 2.21 long-term-support line. Akka 2.10 used Jackson 2.18 which will not get further updates past January 2027.

## Removed deprecated APIs

APIs deprecated in Akka 2.10.0 or earlier have been removed. The sections below list the removed APIs and their replacements.

### akka-cluster-tools

* `DistributedPubSubSettings` constructor without `sendToDeadLettersWhenNoSubscribers`: use the constructor with `sendToDeadLettersWhenNoSubscribers`, or `DistributedPubSubSettings(config)`.

### akka-cluster-typed

* `ClusterSingletonSettings` and `ClusterSingletonManagerSettings` constructors without `leaseSettings`: use the constructor with `leaseSettings`.

### akka-distributed-data

* `ReplicatorSettings` constructors with fewer parameters than the full constructor: use the full constructor, `ReplicatorSettings(config)` or the `with*` methods.
* `ORMap.updated(Cluster, key, initial, modify)` (Java API): use `ORMap.update(SelfUniqueAddress, key, initial, modify)`.
* `GSet.copy$default$1`, a binary compatibility stub in the Scala 2 sources only.
