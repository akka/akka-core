---
project.description: Shard a clustered compute process across the network with locationally transparent message routing using Akka Cluster Sharding.
---
# Cluster Sharding

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Cluster Sharding](../cluster-sharding.md)

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Cluster Sharding, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-sharding-typed_$scala.binary.version$
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster-sharding-typed" }

## Introduction

Cluster sharding is useful when you need to distribute actors across several nodes in the cluster and want to
be able to interact with them using their logical identifier, but without having to care about
their physical location in the cluster, which might also change over time.

It could for example be actors representing Aggregate Roots in Domain-Driven Design terminology.
Here we call these actors "entities". These actors typically have persistent (durable) state,
but this feature is not limited to actors with persistent state.

The [Introduction to Akka Cluster Sharding video](https://www.youtube.com/watch?v=SrPubnOKJcQ)
is a good starting point for learning Cluster Sharding.

Cluster sharding is typically used when you have many stateful actors that together consume
more resources (e.g. memory) than fit on one machine. If you only have a few stateful actors
it might be easier to run them on a @ref:[Cluster Singleton](cluster-singleton.md) node. 

In this context sharding means that actors with an identifier, so called entities,
can be automatically distributed across multiple nodes in the cluster. Each entity
actor runs only at one place, and messages can be sent to the entity without requiring
the sender to know the location of the destination actor. This is achieved by sending
the messages via a `ShardRegion` actor provided by this extension, which knows how
to route the message with the entity id to the final destination.

Cluster sharding will not be active on members with status @ref:[WeaklyUp](cluster-membership.md#weaklyup-members)
if that feature is enabled.

@@@ warning

Make sure to not use a Cluster downing strategy that may split the cluster into several separate clusters in
case of network problems or system overload (long GC pauses), since that will result in *multiple shards and entities*
being started, one in each separate cluster!
See @ref:[Downing](cluster.md#downing).

@@@

## Basic example

Sharding is accessed via the @apidoc[typed.*.ClusterSharding] extension

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #sharding-extension }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #import #sharding-extension }

It is common for sharding to be used with persistence however any @apidoc[typed.Behavior] can be used with sharding e.g. a basic counter:

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #counter }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #counter }

Each Entity type has a key that is then used to retrieve an EntityRef for a given entity identifier. 
Note in the sample's @scala[`Counter.apply`]@java[`Counter.create`] function that the `entityId` parameter is not
called, it is included to demonstrate how one can pass it to an entity. Another way to do this is by sending the `entityId` as part of the message if needed.

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #init }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #init }

Messages to a specific entity are then sent via an @apidoc[typed.*.EntityRef].  The `entityId` and the name of the Entity's key can be retrieved from the `EntityRef`.
It is also possible to wrap methods in a @apidoc[typed.ShardingEnvelope] or define extractor functions and send messages directly to the shard region.

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #send }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #send }

Cluster sharding @apidoc[init](typed.*.ClusterSharding) {scala="#init[M,E](entity:akka.cluster.sharding.typed.scaladsl.Entity[M,E]):akka.actor.typed.ActorRef[E]" java="#init(akka.cluster.sharding.typed.javadsl.Entity)"} should be called on every node for each entity type. Which nodes entity actors are created on
can be controlled with @ref:[roles](cluster.md#node-roles). `init` will create a `ShardRegion` or a proxy depending on whether the node's role matches
the entity's role. 

The behavior factory lambda passed to the init method is defined on each node and only used locally, this means it is safe to use it for injecting for example a node local @apidoc[typed.ActorRef] that each sharded actor should have access to or some object that is not possible to serialize.

Specifying the role:

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #roles }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #roles }

### A note about EntityRef and serialization

If including @apidoc[typed.*.EntityRef]'s in messages or the `State`/`Event`s of an @apidoc[typed.*.EventSourcedBehavior], those `EntityRef`s will need to be serialized.
The @scala[`entityId` and `typeKey` of an `EntityRef`]@java[`getEntityId` and `getTypeKey` methods of an `EntityRef`]
provide exactly the information needed upon deserialization to regenerate an `EntityRef` equivalent to the one serialized, given an expected
type of messages to send to the entity.

At this time, serialization of `EntityRef`s requires a @ref:[custom serializer](../serialization.md#customization), as the specific
@apidoc[EntityTypeKey](typed.*.EntityTypeKey) (including the type of message which the desired entity type accepts) should not simply be encoded in the serialized
representation but looked up on the deserializing side.

## Persistence example

When using sharding, entities can be moved to different nodes in the cluster. Persistence can be used to recover the state of
an actor after it has moved.

Akka Persistence is based on the single-writer principle, for a particular @apidoc[typed.PersistenceId] only one persistent actor
instance should be active. If multiple instances were to persist events at the same time, the events would be
interleaved and might not be interpreted correctly on replay. Cluster Sharding is typically used together with
persistence to ensure that there is only one active entity for each `PersistenceId` (`entityId`).

Here is an example of a persistent actor that is used as a sharded entity:

Scala
:  @@snip [HelloWorldPersistentEntityExample.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/HelloWorldPersistentEntityExample.scala) { #persistent-entity }

Java
:  @@snip [HelloWorldPersistentEntityExample.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/HelloWorldPersistentEntityExample.java) { #persistent-entity-import #persistent-entity }

To initialize and use the entity:

Scala
:  @@snip [HelloWorldPersistentEntityExample.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/HelloWorldPersistentEntityExample.scala) { #persistent-entity-usage }

Java
:  @@snip [HelloWorldPersistentEntityExample.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/HelloWorldPersistentEntityExample.java) { #persistent-entity-usage-import #persistent-entity-usage }

Note how an unique @apidoc[akka.persistence.typed.PersistenceId] can be constructed from the @apidoc[typed.*.EntityTypeKey] and the `entityId`
provided by the @apidoc[typed.*.EntityContext] in the factory function for the @apidoc[typed.Behavior]. This is a typical way
of defining the `PersistenceId` but other formats are possible, as described in the
@ref:[PersistenceId section](persistence.md#persistenceid).

Sending messages to persistent entities is the same as if the entity wasn't persistent. The only difference is
when an entity is moved the state will be restored. In the above example @ref:[ask](interaction-patterns.md#outside-ask)
is used but `tell` or any of the other @ref:[Interaction Patterns](interaction-patterns.md) can be used.

See @ref:[persistence](persistence.md) for more details.

## Shard allocation

A shard is a group of entities that will be managed together. The grouping is typically defined by a hashing
function of the `entityId`. For a specific entity identifier the shard identifier must always
be the same. Otherwise the entity actor might accidentally be started in several places at the same time.

By default the shard identifier is the absolute value of the `hashCode` of the entity identifier modulo
the total number of shards. The number of shards is configured by:

@@snip [reference.conf](/akka-cluster-sharding-typed/src/main/resources/reference.conf) { #number-of-shards }

As a rule of thumb, the number of shards should be a factor ten greater than the planned maximum number of
cluster nodes. It doesn't have to be exact. Fewer shards than number of nodes will result in that some nodes will
not host any shards. Too many shards will result in less efficient management of the shards, e.g. rebalancing overhead,
and increased latency because the coordinator is involved in the routing of the first message for each
shard.

The `number-of-shards` configuration value must be the same for all nodes in the cluster and that is verified by
configuration check when joining. Changing the value requires stopping all nodes in the cluster.

The shards are allocated to the nodes in the cluster. The decision of where to allocate a shard is done
by a shard allocation strategy. 

The default implementation `LeastShardAllocationStrategy` allocates new shards to the `ShardRegion` (node) with least
number of previously allocated shards. This strategy can be replaced by an application specific implementation.

When a node is added to the cluster the shards on the existing nodes will be rebalanced to the new node.
The `LeastShardAllocationStrategy` picks shards for rebalancing from the `ShardRegion`s with most number
of previously allocated shards. They will then be allocated to the `ShardRegion` with least number of
previously allocated shards, i.e. new members in the cluster. The amount of shards to rebalance in each
round can be limited to make it progress slower since rebalancing too many shards at the same time could
result in additional load on the system. For example, causing many Event Sourced entites to be started
at the same time.

A new rebalance algorithm was included in Akka 2.6.10. It can reach optimal balance in a few rebalance rounds
(typically 1 or 2 rounds). For backwards compatibility the new algorithm is not enabled by default.
The new algorithm is recommended and will become the default in future versions of Akka.
You enable the new algorithm by setting `rebalance-absolute-limit` > 0, for example:

```
akka.cluster.sharding.least-shard-allocation-strategy.rebalance-absolute-limit = 20
``` 

The `rebalance-absolute-limit` is the maximum number of shards that will be rebalanced in one rebalance round.

You may also want to tune the `akka.cluster.sharding.least-shard-allocation-strategy.rebalance-relative-limit`.
The `rebalance-relative-limit` is a fraction (< 1.0) of total number of (known) shards that will be rebalanced
in one rebalance round. The lower result of `rebalance-relative-limit` and `rebalance-absolute-limit` will be used.

### External shard allocation

An alternative allocation strategy is the @apidoc[ExternalShardAllocationStrategy] which allows
explicit control over where shards are allocated via the @apidoc[ExternalShardAllocation] extension.

This can be used, for example, to match up Kafka Partition consumption with shard locations. The video [How to co-locate Kafka Partitions with Akka Cluster Shards](https://www.youtube.com/watch?v=Ad2DyOn4dlY) explains a setup for it. Alpakka Kafka provides [an extension for Akka Cluster Sharding](https://doc.akka.io/libraries/alpakka-kafka/current/cluster-sharding.html).

To use it set it as the allocation strategy on your @apidoc[typed.*.Entity]:

Scala
: @@snip [ExternalShardAllocationCompileOnlySpec](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ExternalShardAllocationCompileOnlySpec.scala) { #entity }

Java
: @@snip [ExternalShardAllocationCompileOnlyTest](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ExternalShardAllocationCompileOnlyTest.java) { #entity }

For any shardId that has not been allocated it will be allocated to the requesting node. To make explicit allocations:

Scala
: @@snip [ExternalShardAllocationCompileOnlySpec](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ExternalShardAllocationCompileOnlySpec.scala) { #client }

Java
: @@snip [ExternalShardAllocationCompileOnlyTest](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ExternalShardAllocationCompileOnlyTest.java) { #client }

Any new or moved shard allocations will be moved on the next rebalance.

The communication from the client to the shard allocation strategy is via @ref[Distributed Data](./distributed-data.md).
It uses a single @apidoc[akka.cluster.ddata.LWWMap] that can support 10s of thousands of shards. Later versions could use multiple keys to 
support a greater number of shards.

#### Example project for external allocation strategy

[akka-sample-kafka-to-sharding-scala.zip](../attachments/akka-sample-kafka-to-sharding-scala.zip)
is an example project that can be downloaded, and with instructions of how to run, that demonstrates how to use
external sharding to co-locate Kafka partition consumption with shards.

### Colocate Shards

When using the default shard allocation strategy the shards for different entity types are allocated independent of
each other, i.e. the same shard identifier for the different entity types may be allocated to different nodes.
Colocating shards can be useful if it's known that certain entities interact or share resources with some other
entities and that can be defined by using the same shard identifier.

To colocate such shards you can use the @apidoc[akka.cluster.sharding.ConsistentHashingShardAllocationStrategy].

Let's look at an example where the purpose is to colocate `Device` entities with the `Building` entity they belong to.
To use the same shard identifier we need to use a custom @apidoc[akka.cluster.sharding.typed.ShardingMessageExtractor]
for the `Device` and `Building` entities:

Scala
: @@snip [ConsistentHashingShardAllocationCompileOnlySpec](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ConsistentHashingShardAllocationCompileOnlySpec.scala) { #building #device }

Java
: @@snip [ConsistentHashingShardAllocationCompileOnlyTest](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ConsistentHashingShardAllocationCompileOnlyTest.java) { #building #device }

Set the allocation strategy and message extractor on your @apidoc[typed.*.Entity]:

Scala
: @@snip [ConsistentHashingShardAllocationCompileOnlySpec](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ConsistentHashingShardAllocationCompileOnlySpec.scala) { #init }

Java
: @@snip [ConsistentHashingShardAllocationCompileOnlyTest](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ConsistentHashingShardAllocationCompileOnlyTest.java) { #init }

@@@ note
Create a new instance of the `ConsistentHashingShardAllocationStrategy` for each entity type, i.e. a `ConsistentHashingShardAllocationStrategy` instance must not be shared between different entity types.
@@@

The allocation strategy is using [Consistent Hashing](https://tom-e-white.com/2007/11/consistent-hashing.html)
of the Cluster membership ring to assign a shard to a node. When adding or removing nodes it will rebalance
according to the new consistent hashing, but that means that only a few shards will be rebalanced and others
remain on the same location. When there are changes to the Cluster membership the shards may be on different
nodes for a while, but eventually, when the membership is stable, the shards with the same identifier will
end up on the same node.

### Custom shard allocation

An optional custom shard allocation strategy can be passed into the optional parameter when initializing an entity type 
or explicitly using the @apidoc[withAllocationStrategy](typed.*.Entity) {scala="#withAllocationStrategy(newAllocationStrategy:akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy):akka.cluster.sharding.typed.scaladsl.Entity[M,E]" java="#withAllocationStrategy(akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy)"} function.
See the API documentation of @scala[@scaladoc[ShardAllocationStrategy](akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy)]@java[@javadoc[AbstractShardAllocationStrategy](akka.cluster.sharding.ShardCoordinator.AbstractShardAllocationStrategy)] for details of how to implement a custom `ShardAllocationStrategy`.

## How it works

See @ref:[Cluster Sharding concepts](cluster-sharding-concepts.md).


## Passivation

If the state of the entities are persistent you may stop entities that are not used to
reduce memory consumption. This is done by the application specific implementation of
the entity actors for example by defining receive timeout (@apidoc[context.setReceiveTimeout](typed.*.ActorContext) {scala="#setReceiveTimeout(timeout:scala.concurrent.duration.FiniteDuration,msg:T):Unit" java="#setReceiveTimeout(java.time.Duration,T)"}).
If a message is already enqueued to the entity when it stops itself the enqueued message
in the mailbox will be dropped. To support graceful passivation without losing such
messages the entity actor can send @apidoc[typed.*.ClusterSharding.Passivate] to the
@apidoc[typed.ActorRef]@scala[[@scaladoc[ShardCommand](akka.cluster.sharding.typed.scaladsl.ClusterSharding.ShardCommand)]]@java[<@javadoc[ShardCommand](akka.cluster.sharding.typed.javadsl.ClusterSharding.ShardCommand)>] that was passed in to
the factory method when creating the entity. The optional `stopMessage` message
will be sent back to the entity, which is then supposed to stop itself, otherwise it will
be stopped automatically. Incoming messages will be buffered by the `Shard` between reception
of `Passivate` and termination of the entity. Such buffered messages are thereafter delivered
to a new incarnation of the entity.

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #counter-passivate }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #counter-passivate }

and then initialized with:

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #counter-passivate-init }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #counter-passivate-init }

Note that in the above example the `stopMessage` is specified as `GoodByeCounter`. That message will be sent to
the entity when it's supposed to stop itself due to rebalance or passivation. If the `stopMessage` is not defined
it will be stopped automatically without receiving a specific message. It can be useful to define a custom stop
message if the entity needs to perform some asynchronous cleanup or interactions before stopping.

The stop message is only sent locally, from the shard to the entity so does not require an entity id to end up in the right actor. When using a custom @apidoc[typed.ShardingMessageExtractor] without envelopes, the extractor will still have to handle the stop message type to please the compiler, even though it will never actually be passed to the extractor.

## Automatic Passivation

Entities are automatically passivated based on a passivation strategy. The default passivation strategy is to
[passivate idle entities](#idle-entity-passivation) when they haven't received a message within a specified interval,
and this is the current default strategy to maintain compatibility with earlier versions. It's recommended to switch to
a [passivation strategy with an active entity limit](#active-entity-limits) and a pre-configured default strategy is
provided. Active entity limits and idle entity timeouts can also be used together.

@@@ note

The automatic passivation strategies, except [passivate idle entities](#idle-entity-passivation)
are marked as @ref:[may change](../common/may-change.md) in the sense of being the subject of final development.
This means that the configuration or semantics can change without warning or deprecation period. The passivation
strategies can be used in production, but we reserve the right to adjust the configuration after additional
testing and feedback.

@@@

Automatic passivation can be disabled by setting `akka.cluster.sharding.passivation.strategy = none`. It is disabled
automatically if @ref:[Remembering Entities](#remembering-entities) is enabled.

@@@ note

Only messages sent through Cluster Sharding are counted as entity activity for automatic passivation. Messages sent
directly to the @apidoc[typed.ActorRef], including messages that the actor sends to itself, are not counted as entity activity.

@@@

### Idle entity passivation

Idle entities can be automatically passivated when they have not received a message for a specified length of time.
This is currently the default strategy, for compatibility, and is enabled automatically with a timeout of 2 minutes.
Specify a different idle timeout with configuration:

@@snip [passivation idle timeout](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #passivation-idle-timeout type=conf }

Or specify the idle timeout as a duration using the @apidoc[withPassivationStrategy](typed.ClusterShardingSettings) {scala="#withPassivationStrategy(settings:akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings):akka.cluster.sharding.typed.ClusterShardingSettings" java="#withPassivationStrategy(akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings)"} method on `ClusterShardingSettings`.

Idle entity timeouts can be enabled and configured for any passivation strategy.

### Active entity limits

Automatic passivation strategies can limit the number of active entities. Limit-based passivation strategies use a
replacement policy to determine which active entities should be passivated when the active entity limit is exceeded.
The configurable limit is for a whole shard region and is divided evenly among the active shards in each region.

A recommended passivation strategy, which will become the new default passivation strategy in future versions of Akka
Cluster Sharding, can be enabled with configuration:

@@snip [passivation new default strategy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #passivation-new-default-strategy type=conf }

This default strategy uses a [composite passivation strategy](#composite-passivation-strategies) which combines
recency-based and frequency-based tracking: the main area is configured with a [segmented least recently used
policy](#segmented-least-recently-used-policy) with a frequency-biased [admission filter](#admission-filter), fronted
by a recency-biased [admission window](#admission-window-policy) with [adaptive sizing](#admission-window-optimizer)
enabled.

The active entity limit for the default strategy can be configured:

@@snip [passivation new default strategy configured](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #passivation-new-default-strategy-configured type=conf }

Or using the @apidoc[withActiveEntityLimit](typed.ClusterShardingSettings.PassivationStrategySettings) {scala="#withActiveEntityLimit(limit:Int):akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings" java="#withActiveEntityLimit(int)"} method on `ClusterShardingSettings.PassivationStrategySettings`.

An [idle entity timeout](#idle-entity-passivation) can also be enabled and configured for this strategy:

@@snip [passivation new default strategy with idle](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #passivation-new-default-strategy-with-idle type=conf }

Or using the @apidoc[withIdleEntityPassivation](typed.ClusterShardingSettings.PassivationStrategySettings) {scala="#withIdleEntityPassivation(settings:akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings.IdleSettings):akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings" java="#withIdleEntityPassivation(akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings.IdleSettings)"} method on `ClusterShardingSettings.PassivationStrategySettings`.

If the default strategy is not appropriate for particular workloads and access patterns, a [custom passivation
strategy](#custom-passivation-strategies) can be created with configurable replacement policies, active entity limits,
and idle entity timeouts.

### Custom passivation strategies

To configure a custom passivation strategy, create a configuration section for the strategy under
`akka.cluster.sharding.passivation` and select this strategy using the `strategy` setting. The strategy needs a
_replacement policy_ to be chosen, an _active entity limit_ to be set, and can optionally [passivate idle
entities](#idle-entity-passivation). For example, a custom strategy can be configured to use the [least recently used
policy](#least-recently-used-policy):

@@snip [custom passivation strategy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #custom-passivation-strategy type=conf }

The active entity limit and replacement policy can also be configured using the `withPassivationStrategy` method on
`ClusterShardingSettings`, passing custom `ClusterShardingSettings.PassivationStrategySettings`.

### Least recently used policy

The **least recently used** policy passivates those entities that have the least recent activity when the number of
active entities passes the specified limit.

**When to use**: the least recently used policy should be used when access patterns are recency biased, where entities
that were recently accessed are likely to be accessed again. See the [segmented least recently used
policy](#segmented-least-recently-used-policy) for a variation that also distinguishes frequency of access.

Configure a passivation strategy to use the least recently used policy:

@@snip [LRU policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #lru-policy type=conf }

Or using the @apidoc[withLeastRecentlyUsedReplacement](typed.ClusterShardingSettings.PassivationStrategySettings) {scala="#withLeastRecentlyUsedReplacement():akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings" java="#withLeastRecentlyUsedReplacement()"} method on `ClusterShardingSettings.PassivationStrategySettings`.

#### Segmented least recently used policy

A variation of the least recently used policy can be enabled that divides the active entity space into multiple
segments to introduce frequency information into the passivation strategy. Higher-level segments contain entities that
have been accessed more often. The first segment is for entities that have only been accessed once, the second segment
for entities that have been accessed at least twice, and so on. When an entity is accessed again, it will be promoted
to the most recent position of the next-level or highest-level segment. The higher-level segments are limited, where
the total limit is either evenly divided among segments, or proportions of the segments can be configured. When a
higher-level segment exceeds its limit, the least recently used active entity tracked in that segment will be demoted
to the level below. Only the least recently used entities in the lowest level will be candidates for passivation. The
higher levels are considered "protected", where entities will have additional opportunities to be accessed before being
considered for passivation.

**When to use**: the segmented least recently used policy can be used for workloads where some entities are more
popular than others, to prioritize those entities that are accessed more frequently.

To configure a segmented least recently used (SLRU) policy, with two levels and a protected segment limited to 80% of
the total limit:

@@snip [SLRU policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #slru-policy type=conf }

Or to configure a 4-level segmented least recently used (S4LRU) policy, with 4 evenly divided levels:

@@snip [S4LRU policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #s4lru-policy type=conf }

Or using custom `ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings`.

### Most recently used policy

The **most recently used** policy passivates those entities that have the most recent activity when the number of
active entities passes the specified limit.

**When to use**: the most recently used policy is most useful when the older an entity is, the more likely that entity
will be accessed again; as seen in cyclic access patterns.

Configure a passivation strategy to use the most recently used policy:

@@snip [MRU policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #mru-policy type=conf }

Or using the @apidoc[withMostRecentlyUsedReplacement](typed.ClusterShardingSettings.PassivationStrategySettings) {scala="#withMostRecentlyUsedReplacement():akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings" java="#withMostRecentlyUsedReplacement()"} method on `ClusterShardingSettings.PassivationStrategySettings`.

### Least frequently used policy

The **least frequently used** policy passivates those entities that have the least frequent activity when the number of
active entities passes the specified limit.

**When to use**: the least frequently used policy should be used when access patterns are frequency biased, where some
entities are much more popular than others and should be prioritized. See the [least frequently used with dynamic aging
policy](#least-frequently-used-with-dynamic-aging-policy) for a variation that also handles shifts in popularity.

Configure automatic passivation to use the least frequently used policy:

@@snip [LFU policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #lfu-policy type=conf }

Or using the @apidoc[withLeastFrequentlyUsedReplacement](typed.ClusterShardingSettings.PassivationStrategySettings) {scala="#withLeastFrequentlyUsedReplacement():akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings" java="#withLeastFrequentlyUsedReplacement()"} method on `ClusterShardingSettings.PassivationStrategySettings`.

#### Least frequently used with dynamic aging policy

A variation of the least frequently used policy can be enabled that uses "dynamic aging" to adapt to shifts in the set
of popular entities, which is useful for smaller active entity limits and when shifts in popularity are common. If
entities were frequently accessed in the past but then become unpopular, they can still remain active for a long time
given their high frequency counts. Dynamic aging effectively increases the frequencies for recently accessed entities
so they can more easily become higher priority over entities that are no longer accessed.

**When to use**: the least frequently used with dynamic aging policy can be used when workloads are frequency biased
(there are some entities that are much more popular), but which entities are most popular changes over time. Shifts in
popularity can have more impact on a least frequently used policy if the active entity limit is small.

Configure dynamic aging with the least frequently used policy:

@@snip [LFUDA policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #lfuda-policy type=conf }

Or using custom `ClusterShardingSettings.PassivationStrategySettings.LeastFrequentlyUsedSettings`.

### Composite passivation strategies

Passivation strategies can be combined using an admission window and admission filter. The admission window tracks
newly activated entities. Entities are replaced in the admission window using one of the replacement policies, such as
the least recently used replacement policy. When an entity is replaced in the window area it has an opportunity to
enter the main entity tracking area, based on the admission filter. The admission filter determines whether an entity
that has left the window area should be admitted into the main area, or otherwise be passivated. A frequency sketch is
the default admission filter and estimates the access frequency of entities over the lifespan of the cluster sharding
node, selecting the entity that is estimated to be accessed more frequently. Composite passivation strategies with an
admission window and admission filter are implementing the _Window-TinyLFU_ caching algorithm.

#### Admission window policy

The admission window tracks newly activated entities. When an entity is replaced in the window area, it has an
opportunity to enter the main entity tracking area, based on the [admission filter](#admission-filter). The admission
window can be enabled by selecting a policy (while the regular replacement policy is for the main area):

@@snip [admission window policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #admission-window-policy type=conf }

The proportion of the active entity limit used for the admission window can be configured (the default is 1%):

@@snip [admission window proportion](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #admission-window-proportion type=conf }

The proportion for the admission window can also be adapted and optimized dynamically, by enabling an [admission window
optimizer](#admission-window-optimizer).

#### Admission window optimizer

The proportion of the active entity limit used for the admission window can be adapted dynamically using an optimizer.
The window area will usually retain entities that are accessed again in a short time (recency-biased), while the main
area can track entities that are accessed more frequently over longer times (frequency-biased). If access patterns for
entities are changeable, then the adaptive sizing of the window allows the passivation strategy to adapt between
recency-biased and frequency-biased workloads.

The optimizer currently available uses a simple hill-climbing algorithm, which searches for a window proportion that
provides an optimal active rate (where entities are already active when accessed, the _cache hit rate_). Enable
adaptive window sizing by configuring the `hill-climbing` window optimizer:

@@snip [admission window optimizer](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #admission-window-optimizer type=conf }

See the `reference.conf` for parameters that can be tuned for the hill climbing admission window optimizer.

#### Admission filter

An admission filter can be enabled, which determines whether an entity that has left the window area (or a newly
activated entity if there is no admission window) should be admitted into the main entity tracking area, or otherwise
be passivated. If no admission filter is configured, then entities will always be admitted into the main area.

A frequency sketch is the default admission filter and estimates the access frequency of entities over the lifespan of
the cluster sharding node, selecting the entity that is estimated to be accessed more frequently. The frequency sketch
automatically ages entries, using the approach from the _TinyLFU_ cache admission algorithm. Enable an admission filter
by configuring the `frequency-sketch` admission filter:

@@snip [admission policy](/akka-cluster-sharding/src/test/scala/akka/cluster/sharding/ClusterShardingSettingsSpec.scala) { #admission-policy type=conf }

See the `reference.conf` for parameters that can be tuned for the frequency sketch admission filter.


## Sharding State 

There are two types of state managed:

1. @ref:[ShardCoordinator State](cluster-sharding-concepts.md#shardcoordinator-state) - the `Shard` locations. This is stored in the `State Store`.
1. @ref:[Remembering Entities](#remembering-entities) - the active shards and the entities in each `Shard`, which is optional, and disabled by default. This is stored in the `Remember Entities Store`. 
 

### State Store

A state store is mandatory for sharding, it contains the location of shards. The `ShardCoordinator` needs to load this state after
it moves between nodes.

There are two options for the state store:

* @ref:[Distributed Data Mode](#distributed-data-mode) - uses Akka @ref:[Distributed Data](distributed-data.md) (CRDTs) (the default)
* @ref:[Persistence Mode](#persistence-mode) - (deprecated) uses Akka @ref:[Persistence](persistence.md) (Event Sourcing)

@@include[cluster.md](../includes/cluster.md) { #sharding-persistence-mode-deprecated }

#### Distributed Data Mode

To enable distributed data store mode (the default):

```
akka.cluster.sharding.state-store-mode = ddata
```

The state of the `ShardCoordinator` is replicated across the cluster but is not stored to disk.
@ref:[Distributed Data](distributed-data.md) handles the `ShardCoordinator`'s state with @apidoc[WriteMajorityPlus](Replicator.WriteMajorityPlus) / @apidoc[ReadMajorityPlus](Replicator.ReadMajorityPlus) consistency.
When all nodes in the cluster have been stopped, the state is no longer needed and dropped.

Cluster Sharding uses its own Distributed Data @apidoc[ddata.Replicator] per node. 
If using roles with sharding there is one `Replicator` per role, which enables a subset of
all nodes for some entity types and another subset for other entity types. Each replicator has a name
that contains the node role and therefore the role configuration must be the same on all nodes in the
cluster, for example you can't change the roles when performing a rolling update.
Changing roles requires @ref:[a full cluster restart](../additional/rolling-updates.md#cluster-sharding-configuration-change).

The `akka.cluster.sharding.distributed-data` config section configures the settings for Distributed Data. 
It's not possible to have different `distributed-data` settings for different sharding entity types.

#### Persistence mode

To enable persistence store mode:

```
akka.cluster.sharding.state-store-mode = persistence
```

Since it is running in a cluster @ref:[Persistence](persistence.md) must be configured with a distributed journal.

@@@ warning 

Persistence mode for @ref:[Remembering Entities](#remembering-entities) has been replaced by a remember entities state mode. It should not be
used for new projects and existing projects should migrate as soon as possible.

@@@

## Remembering Entities

Remembering entities automatically restarts entities after a rebalance or entity crash. 
Without remembered entities restarts happen on the arrival of a message.

Enabling remembered entities disables @ref:[Automatic Passivation](#automatic-passivation).

The state of the entities themselves is not restored unless they have been made persistent,
for example with @ref:[Event Sourcing](persistence.md).

To enable remember entities set `rememberEntities` flag to true in
@apidoc[typed.ClusterShardingSettings] when starting a shard region (or its proxy) for a given `entity` type or configure
`akka.cluster.sharding.remember-entities = on`.

Starting and stopping entities has an overhead but this is limited by batching operations to the
underlying remember entities store.

### Behavior When Enabled 

When `rememberEntities` is enabled, whenever a `Shard` is rebalanced onto another
node or recovers after a crash, it will recreate all the entities which were previously
running in that `Shard`. 

To permanently stop entities send a @apidoc[ClusterSharding.Passivate](typed.*.ClusterSharding.Passivate) to the
@apidoc[typed.ActorRef]@scala[[@scaladoc[ShardCommand](akka.cluster.sharding.typed.scaladsl.ClusterSharding.ShardCommand)]]@java[<@javadoc[ShardCommand](akka.cluster.sharding.typed.javadsl.ClusterSharding.ShardCommand)>] that was passed in to
the factory method when creating the entity.
Otherwise, the entity will be automatically restarted after the entity restart backoff specified in the configuration.

### Remember entities store

There are two options for the remember entities store:

1. `ddata` 
1. `eventsourced` 

#### Remember entities distributed data mode

Enable ddata mode with (enabled by default):

```
akka.cluster.sharding.remember-entities-store = ddata
```

To support restarting entities after a full cluster restart (non-rolling) the remember entities store is persisted to disk by distributed data.
This can be disabled if not needed:
```
akka.cluster.sharding.distributed-data.durable.keys = []
```

Reasons for disabling:

* No requirement for remembering entities after a full cluster shutdown
* Running in an environment without access to disk between restarts e.g. Kubernetes without persistent volumes

For supporting remembered entities in an environment without disk storage use `eventsourced` mode instead.

@@@ note { title="Java 17" }

When using `remember-entities-store=ddata` the remember entities store is persisted to disk by LMDB.
When running with Java 17 you have to add JVM flags `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`.

@@@

#### Event sourced mode

Enable `eventsourced` mode with:

```
akka.cluster.sharding.remember-entities-store = eventsourced
```

This mode uses @ref:[Event Sourcing](./persistence.md) to store the active shards and active entities for each shard 
so a persistence and snapshot plugin must be configured.

```
akka.cluster.sharding.journal-plugin-id = <plugin>
akka.cluster.sharding.snapshot-plugin-id = <plugin>
```

### Migrating from deprecated persistence mode

If not using remembered entities you can migrate to ddata with a full cluster restart.

If using remembered entities there are two migration options: 

* `ddata` for the state store and `ddata` for remembering entities. All remembered entities will be lost after a full cluster restart.
* `ddata` for the state store and `eventsourced` for remembering entities. The new `eventsourced` remembering entities store 
   reads the data written by the old `persistence` mode. Your remembered entities will be remembered after a full cluster restart. 

For migrating existing remembered entities an event adapter needs to be configured in the config for the journal you use in your `application.conf`.
In this example `cassandra` is the used journal:

```
akka.persistence.cassandra.journal {
  event-adapters {
    coordinator-migration = "akka.cluster.sharding.OldCoordinatorStateMigrationEventAdapter"
  }

  event-adapter-bindings {
    "akka.cluster.sharding.ShardCoordinator$Internal$DomainEvent" = coordinator-migration
  }
}
```

Once you have migrated you cannot go back to the old persistence store, a rolling update is therefore not possible.

When @ref:[Distributed Data mode](#distributed-data-mode) is used the identifiers of the entities are
stored in @ref:[Durable Storage](distributed-data.md#durable-storage) of Distributed Data. You may want to change the
configuration of the `akka.cluster.sharding.distributed-data.durable.lmdb.dir`, since
the default directory contains the remote port of the actor system. If using a dynamically
assigned port (0) it will be different each time and the previously stored data will not
be loaded.

The reason for storing the identifiers of the active entities in durable storage, i.e. stored to
disk, is that the same entities should be started also after a complete cluster restart. If this is not needed
you can disable durable storage and benefit from better performance by using the following configuration:

```
akka.cluster.sharding.distributed-data.durable.keys = []
```
## Startup after minimum number of members

It's recommended to use Cluster Sharding with the Cluster setting `akka.cluster.min-nr-of-members` or
`akka.cluster.role.<role-name>.min-nr-of-members`. `min-nr-of-members` will defer the allocation of the shards
until at least that number of regions have been started and registered to the coordinator. This
avoids that many shards are allocated to the first region that registers and only later are
rebalanced to other nodes.

See @ref:[How To Startup when Cluster Size Reached](cluster.md#how-to-startup-when-a-cluster-size-is-reached)
for more information about `min-nr-of-members`.

## Health check

An @extref:[Akka Management compatible health check](akka-management:healthchecks.html) is included that returns healthy once the local shard region
has registered with the coordinator. This health check should be used in cases where you don't want to receive production traffic until the local shard region is ready to retrieve locations
for shards. For shard regions that aren't critical and therefore should not block this node becoming ready do not include them.

The health check does not fail after an initial successful check. Once a shard region is registered and is operational it stays available for incoming message. 

Cluster sharding enables the health check automatically. To disable:

```ruby
akka.management.health-checks.readiness-checks {
  sharding = ""
}
```

Monitoring of each shard region is off by default. Add them by defining the entity type names (`EntityTypeKey.name`):

```ruby
akka.cluster.sharding.healthcheck.names = ["counter-1", "HelloWorld"]
```

The health check is disabled (always returns success true) after a duration of failing checks after the Cluster member is up. Otherwise, it would stall a Kubernetes rolling update when adding a new entity type in the new version.

See also additional information about how to make @ref:[smooth rolling updates](../additional/rolling-updates.md#cluster-sharding).

## Inspecting cluster sharding state

Two requests to inspect the cluster state are available:

@apidoc[akka.cluster.sharding.typed.GetShardRegionState] which will reply with a 
@scala[@scaladoc[ShardRegion.CurrentShardRegionState](akka.cluster.sharding.ShardRegion.CurrentShardRegionState)]@java[@javadoc[ShardRegion.CurrentShardRegionState](akka.cluster.sharding.ShardRegion$)]
that contains the identifiers of the shards running in a Region and what entities are alive for each of them.

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #get-shard-region-state }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #get-shard-region-state }

@apidoc[akka.cluster.sharding.typed.GetClusterShardingStats] which will query all the regions in the cluster and reply with a
@scala[@scaladoc[ShardRegion.ClusterShardingStats](akka.cluster.sharding.ShardRegion.ClusterShardingStats)]@java[@javadoc[ShardRegion.ClusterShardingStats](akka.cluster.sharding.ShardRegion$)]
containing the identifiers of the shards running in each region and a count of entities that are alive in each shard.

Scala
:  @@snip [ShardingCompileOnlySpec.scala](/akka-cluster-sharding-typed/src/test/scala/docs/akka/cluster/sharding/typed/ShardingCompileOnlySpec.scala) { #get-cluster-sharding-stats }

Java
:  @@snip [ShardingCompileOnlyTest.java](/akka-cluster-sharding-typed/src/test/java/jdocs/akka/cluster/sharding/typed/ShardingCompileOnlyTest.java) { #get-cluster-sharding-stats }

If any shard queries failed, for example due to timeout if a shard was too busy to reply within the configured `akka.cluster.sharding.shard-region-query-timeout`, 
`ShardRegion.CurrentShardRegionState` and `ShardRegion.ClusterShardingStats` will also include the set of shard identifiers by region that failed.

The purpose of these messages is testing and monitoring, they are not provided to give access to
directly sending messages to the individual entities.

## Lease

A @ref[lease](../coordination.md) can be used as an additional safety measure to ensure a shard 
does not run on two nodes.

Reasons for how this can happen:

* Network partitions without an appropriate downing provider
* Mistakes in the deployment process leading to two separate Akka Clusters
* Timing issues between removing members from the Cluster on one side of a network partition and shutting them down on the other side

A lease can be a final backup that means that each shard won't create child entity actors unless it has the lease. 

To use a lease for sharding set `akka.cluster.sharding.use-lease` to the configuration location
of the lease to use. Each shard will try and acquire a lease with with the name `<actor system name>-shard-<type name>-<shard id>` and
the owner is set to the `Cluster(system).selfAddress.hostPort`.

If a shard can't acquire a lease it will remain uninitialized so messages for entities it owns will
be buffered in the `ShardRegion`. If the lease is lost after initialization the Shard will be terminated.

## Removal of internal Cluster Sharding data

Removal of internal Cluster Sharding data is only relevant for "Persistent Mode".
The Cluster Sharding `ShardCoordinator` stores locations of the shards.
This data is safely be removed when restarting the whole Akka Cluster.
Note that this does not include application data.

There is a utility program @apidoc[akka.cluster.sharding.RemoveInternalClusterShardingData$]
that removes this data.

@@@ warning

Never use this program while there are running Akka Cluster nodes that are
using Cluster Sharding. Stop all Cluster nodes before using this program.

@@@

It can be needed to remove the data if the Cluster Sharding coordinator
cannot startup because of corrupt data, which may happen if accidentally
two clusters were running at the same time, e.g. caused by an invalid downing
provider when there was a network partition.

Use this program as a standalone Java main program:

```
java -classpath <jar files, including akka-cluster-sharding>
  akka.cluster.sharding.RemoveInternalClusterShardingData
    -2.3 entityType1 entityType2 entityType3
```

The program is included in the `akka-cluster-sharding` jar file. It
is easiest to run it with same classpath and configuration as your ordinary
application. It can be run from sbt or Maven in similar way.

Specify the entity type names (same as you use in the `init` method
of `ClusterSharding`) as program arguments.

If you specify `-2.3` as the first program argument it will also try
to remove data that was stored by Cluster Sharding in Akka 2.3.x using
different persistenceId.

## Configuration

The @apidoc[typed.*.ClusterSharding] extension can be configured with the following properties. These configuration
properties are read by the @apidoc[typed.ClusterShardingSettings] when created with an ActorSystem parameter.
It is also possible to amend the `ClusterShardingSettings` or create it from another config section
with the same layout as below. 

One important configuration property is `number-of-shards` as described in @ref:[Shard allocation](#shard-allocation).

You may also need to tune the configuration properties is `rebalance-absolute-limit` and `rebalance-relative-limit`
as described in @ref:[Shard allocation](#shard-allocation).

@@snip [reference.conf](/akka-cluster-sharding/src/main/resources/reference.conf) { #sharding-ext-config }

@@snip [reference.conf](/akka-cluster-sharding-typed/src/main/resources/reference.conf) { #sharding-ext-config }

## Example project

@java[[Sharding example project](../attachments/akka-sample-sharding-java.zip)]
@scala[[Sharding example project](../attachments/akka-sample-sharding-scala.zip)]
is an example project that can be downloaded, and with instructions of how to run.

This project contains a KillrWeather sample illustrating how to use Cluster Sharding.
