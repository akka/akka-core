# Classic Cluster Sharding

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[Cluster Sharding](typed/cluster-sharding.md).

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Cluster Sharding, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-sharding_$scala.binary.version$
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster-sharding" }

## Introduction

For an introduction to Sharding concepts see @ref:[Cluster Sharding](typed/cluster-sharding.md).

## Basic example

This is what an entity actor may look like:

Scala
:  @@snip [ClusterShardingSpec.scala](/akka-cluster-sharding/src/multi-jvm/scala/akka/cluster/sharding/ClusterShardingSpec.scala) { #counter-actor }

Java
:  @@snip [ClusterShardingTest.java](/akka-docs/src/test/java/jdocs/sharding/ClusterShardingTest.java) { #counter-actor }

The above actor uses Event Sourcing and the support provided in @scala[`PersistentActor`] @java[`AbstractPersistentActor`] to store its state.
It does not have to be a persistent actor, but in case of failure or migration of entities between nodes it must be able to recover
its state if it is valuable.

Note how the `persistenceId` is defined. The name of the actor is the entity identifier (utf-8 URL-encoded).
You may define it another way, but it must be unique.

When using the sharding extension you are first, typically at system startup on each node
in the cluster, supposed to register the supported entity types with the `ClusterSharding.start`
method. `ClusterSharding.start` gives you the reference which you can pass along.
Please note that `ClusterSharding.start` will start a `ShardRegion` in @ref:[proxy only mode](#proxy-only-mode) 
when there is no match between the roles of the current cluster node and the role specified in 
`ClusterShardingSettings`.

Scala
:  @@snip [ClusterShardingSpec.scala](/akka-cluster-sharding/src/multi-jvm/scala/akka/cluster/sharding/ClusterShardingSpec.scala) { #counter-start }

Java
:  @@snip [ClusterShardingTest.java](/akka-docs/src/test/java/jdocs/sharding/ClusterShardingTest.java) { #counter-start }

The @scala[`extractEntityId` and `extractShardId` are two] @java[`messageExtractor` defines] application specific @scala[functions] @java[methods] to extract the entity
identifier and the shard identifier from incoming messages.

Scala
:  @@snip [ClusterShardingSpec.scala](/akka-cluster-sharding/src/multi-jvm/scala/akka/cluster/sharding/ClusterShardingSpec.scala) { #counter-extractor }

Java
:  @@snip [ClusterShardingTest.java](/akka-docs/src/test/java/jdocs/sharding/ClusterShardingTest.java) { #counter-extractor }

This example illustrates two different ways to define the entity identifier in the messages:

 * The `Get` message includes the identifier itself.
 * The `EntityEnvelope` holds the identifier, and the actual message that is
sent to the entity actor is wrapped in the envelope.

Note how these two messages types are handled in the @scala[`extractEntityId` function] @java[`entityId` and `entityMessage` methods] shown above.
The message sent to the entity actor is @scala[the second part of the tuple returned by the `extractEntityId`] @java[what `entityMessage` returns] and that makes it possible to unwrap envelopes
if needed.

A shard is a group of entities that will be managed together. The grouping is defined by the
`extractShardId` function shown above. For a specific entity identifier the shard identifier must always
be the same. Otherwise the entity actor might accidentally be started in several places at the same time.

Creating a good sharding algorithm is an interesting challenge in itself. Try to produce a uniform distribution,
i.e. same amount of entities in each shard. As a rule of thumb, the number of shards should be a factor ten greater
than the planned maximum number of cluster nodes. Fewer shards than number of nodes will result in that some nodes
will not host any shards. Too many shards will result in less efficient management of the shards, e.g. rebalancing
overhead, and increased latency because the coordinator is involved in the routing of the first message for each
shard. The sharding algorithm must be the same on all nodes in a running cluster. It can be changed after stopping
all nodes in the cluster.

A simple sharding algorithm that works fine in most cases is to take the absolute value of the `hashCode` of
the entity identifier modulo number of shards. As a convenience this is provided by the
`ShardRegion.HashCodeMessageExtractor`.

Messages to the entities are always sent via the local `ShardRegion`. The `ShardRegion` actor reference for a
named entity type is returned by `ClusterSharding.start` and it can also be retrieved with `ClusterSharding.shardRegion`.
The `ShardRegion` will lookup the location of the shard for the entity if it does not already know its location. It will
delegate the message to the right node and it will create the entity actor on demand, i.e. when the
first message for a specific entity is delivered.

Scala
:  @@snip [ClusterShardingSpec.scala](/akka-cluster-sharding/src/multi-jvm/scala/akka/cluster/sharding/ClusterShardingSpec.scala) { #counter-usage }

Java
:  @@snip [ClusterShardingTest.java](/akka-docs/src/test/java/jdocs/sharding/ClusterShardingTest.java) { #counter-usage }

## How it works

See @ref:[Cluster Sharding concepts](typed/cluster-sharding-concepts.md) in the documentation of the new APIs.

<a id="cluster-sharding-mode"></a>
## Sharding State Store Mode

There are two cluster sharding states managed:

1. @ref:[ShardCoordinator State](typed/cluster-sharding-concepts.md#shardcoordinator-state) - the `Shard` locations
1. @ref:[Remembering Entities](#remembering-entities) - the entities in each `Shard`, which is optional, and disabled by default
 
For these, there are currently two modes which define how these states are stored:

* @ref:[Distributed Data Mode](#distributed-data-mode) - uses Akka @ref:[Distributed Data](distributed-data.md) (CRDTs) (the default)
* @ref:[Persistence Mode](#persistence-mode) - (deprecated) uses Akka @ref:[Persistence](persistence.md) (Event Sourcing)

@@include[cluster.md](includes/cluster.md) { #sharding-persistence-mode-deprecated }
 
Changing the mode requires @ref:[a full cluster restart](additional/rolling-updates.md#cluster-sharding-configuration-change).

### Distributed Data Mode

The state of the `ShardCoordinator` is replicated across the cluster but is not durable, not stored to disk.
The `ShardCoordinator` state replication is handled by @ref:[Distributed Data](distributed-data.md) with `WriteMajority`/`ReadMajority` consistency.
When all nodes in the cluster have been stopped, the state is no longer needed and dropped.

See @ref:[Distributed Data mode](typed/cluster-sharding.md#distributed-data-mode) in the documentation of the new APIs.

### Persistence Mode

See @ref:[Persistence Mode](typed/cluster-sharding.md#persistence-mode) in the documentation of the new APIs.

## Proxy Only Mode

The `ShardRegion` actor can also be started in proxy only mode, i.e. it will not
host any entities itself, but knows how to delegate messages to the right location.
A `ShardRegion` is started in proxy only mode with the `ClusterSharding.startProxy` method.
Also a `ShardRegion` is started in proxy only mode when there is no match between the
roles of the current cluster node and the role specified in `ClusterShardingSettings` 
passed to the `ClusterSharding.start` method.

## Passivation

If the state of the entities are persistent you may stop entities that are not used to
reduce memory consumption. This is done by the application specific implementation of
the entity actors for example by defining receive timeout (`context.setReceiveTimeout`).
If a message is already enqueued to the entity when it stops itself the enqueued message
in the mailbox will be dropped. To support graceful passivation without losing such
messages the entity actor can send `ShardRegion.Passivate` to its parent `Shard`.
The specified wrapped message in `Passivate` will be sent back to the entity, which is
then supposed to stop itself. Incoming messages will be buffered by the `Shard`
between reception of `Passivate` and termination of the entity. Such buffered messages
are thereafter delivered to a new incarnation of the entity.

See @ref:[Automatic Passivation](typed/cluster-sharding.md#automatic-passivation) in the documentation of the new APIs.
 
<a id="cluster-sharding-remembering"></a>
## Remembering Entities

See @ref:[Remembering Entities](typed/cluster-sharding.md#remembering-entities) in the documentation of the new APIs, 
including behavior when enabled and disabled.
 
Note that the state of the entities themselves will not be restored unless they have been made persistent,
for example with @ref:[Event Sourcing](persistence.md).

To make the list of entities in each `Shard` persistent (durable), set 
the `rememberEntities` flag to true in `ClusterShardingSettings` when calling
`ClusterSharding.start` and make sure the `shardIdExtractor` handles
`Shard.StartEntity(EntityId)` which implies that a `ShardId` must be possible to
extract from the `EntityId`.

Scala
:  @@snip [ClusterShardingSpec.scala](/akka-cluster-sharding/src/multi-jvm/scala/akka/cluster/sharding/ClusterShardingSpec.scala) { #extractShardId-StartEntity }

Java
:  @@snip [ClusterShardingTest.java](/akka-docs/src/test/java/jdocs/sharding/ClusterShardingTest.java) { #extractShardId-StartEntity }
 
## Supervision

If you need to use another `supervisorStrategy` for the entity actors than the default (restarting) strategy
you need to create an intermediate parent actor that defines the `supervisorStrategy` to the
child entity actor.

Scala
:  @@snip [ClusterShardingSpec.scala](/akka-cluster-sharding/src/multi-jvm/scala/akka/cluster/sharding/ClusterShardingSpec.scala) { #supervisor }

Java
:  @@snip [ClusterShardingTest.java](/akka-docs/src/test/java/jdocs/sharding/ClusterShardingTest.java) { #supervisor }

You start such a supervisor in the same way as if it was the entity actor.

Scala
:  @@snip [ClusterShardingSpec.scala](/akka-cluster-sharding/src/multi-jvm/scala/akka/cluster/sharding/ClusterShardingSpec.scala) { #counter-supervisor-start }

Java
:  @@snip [ClusterShardingTest.java](/akka-docs/src/test/java/jdocs/sharding/ClusterShardingTest.java) { #counter-supervisor-start }

Note that stopped entities will be started again when a new message is targeted to the entity.

If 'on stop' backoff supervision strategy is used, a final termination message must be set and used for passivation, see @ref:[Backoff supervisor and sharding](fault-tolerance.md#sharding)

## Graceful Shutdown

You can send the @scala[`ShardRegion.GracefulShutdown`] @java[`ShardRegion.gracefulShutdownInstance`] message
to the `ShardRegion` actor to hand off all shards that are hosted by that `ShardRegion` and then the
`ShardRegion` actor will be stopped. You can `watch` the `ShardRegion` actor to know when it is completed.
During this period other regions will buffer messages for those shards in the same way as when a rebalance is
triggered by the coordinator. When the shards have been stopped the coordinator will allocate these shards elsewhere.

This is performed automatically by the @ref:[Coordinated Shutdown](coordinated-shutdown.md) and is therefore part of the
graceful leaving process of a cluster member.

<a id="removeinternalclustershardingdata"></a>
## Removal of Internal Cluster Sharding Data

See @ref:[removal of Internal Cluster Sharding Data](typed/cluster-sharding.md#removal-of-internal-cluster-sharding-data) in the documentation of the new APIs.

## Inspecting cluster sharding state

Two requests to inspect the cluster state are available:

@scala[`ShardRegion.GetShardRegionState`] @java[`ShardRegion.getShardRegionStateInstance`] which will return
a @scala[`ShardRegion.CurrentShardRegionState`] @java[`ShardRegion.ShardRegionState`] that contains
the identifiers of the shards running in a Region and what entities are alive for each of them. 

`ShardRegion.GetClusterShardingStats` which will query all the regions in the cluster and return
a `ShardRegion.ClusterShardingStats` containing the identifiers of the shards running in each region and a count
of entities that are alive in each shard. 

If any shard queries failed, for example due to timeout if a shard was too busy to reply within the configured `akka.cluster.sharding.shard-region-query-timeout`, 
`ShardRegion.CurrentShardRegionState` and `ShardRegion.ClusterShardingStats` will also include the set of shard identifiers by region that failed.

The type names of all started shards can be acquired via @scala[`ClusterSharding.shardTypeNames`]  @java[`ClusterSharding.getShardTypeNames`].

The purpose of these messages is testing and monitoring, they are not provided to give access to
directly sending messages to the individual entities.

## Lease

A lease can be used as an additional safety measure to ensure a shard does not run on two nodes.
See @ref:[Lease](typed/cluster-sharding.md#lease) in the documentation of the new APIs.

## Configuration

`ClusterShardingSettings` is a parameter to the `start` method of
the `ClusterSharding` extension, i.e. each each entity type can be configured with different settings
if needed.

See @ref:[configuration](typed/cluster-sharding.md#configuration) for more information.
