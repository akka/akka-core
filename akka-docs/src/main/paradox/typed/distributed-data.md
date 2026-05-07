---
project.description: Share data between nodes and perform updates without coordination in an Akka Cluster using Conflict Free Replicated Data Types CRDT.
---
# Distributed Data

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Distributed Data](../distributed-data.md).

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Cluster Distributed Data, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-typed_$scala.binary.version$
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster-typed" }

## Introduction

*Akka Distributed Data* is useful when you need to share data between nodes in an
Akka Cluster. The data is accessed with an actor providing a key-value store like API.
The keys are unique identifiers with type information of the data values. The values
are *Conflict Free Replicated Data Types* (CRDTs).

All data entries are spread to all nodes, or nodes with a certain role, in the cluster
via direct replication and gossip based dissemination. You have fine grained control
of the consistency level for reads and writes.

The nature of CRDTs makes it possible to perform updates from any node without coordination.
Concurrent updates from different nodes will automatically be resolved by the monotonic
merge function, which all data types must provide. The state changes always converge.
Several useful data types for counters, sets, maps and registers are provided and
you can also implement your own custom data types.

It is eventually consistent and geared toward providing high read and write availability
(partition tolerance), with low latency. Note that in an eventually consistent system a read may return an
out-of-date value.

## Using the Replicator

You can interact with the data through the replicator actor which can be
accessed through the @apidoc[typed.*.DistributedData] extension.

The messages for the replicator, such as @apidoc[typed.*Replicator.Update] are defined as
subclasses of @apidoc[typed.*Replicator.Command]
and the actual CRDTs are defined in the `akka.cluster.ddata` package, for example
@apidoc[akka.cluster.ddata.GCounter]. It requires a @scala[implicit] `akka.cluster.ddata.SelfUniqueAddress`,
available from:

Scala
:  @@snip [ReplicatorSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/ddata/typed/scaladsl/ReplicatorDocSpec.scala) { #selfUniqueAddress }

Java
:  @@snip [ReplicatorTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/ddata/typed/javadsl/ReplicatorDocSample.java) { #selfUniqueAddress }
 
The replicator can contain multiple entries each containing a replicated data type, we therefore need to create a 
key identifying the entry and helping us know what type it has, and then use that key for every interaction with
the replicator. Each replicated data type contains a factory for defining such a key.

Cluster members with status @ref:[WeaklyUp](cluster-membership.md#weakly-up),
will participate in Distributed Data. This means that the data will be replicated to the
`WeaklyUp` nodes with the background gossip protocol. Note that it
will not participate in any actions where the consistency mode is to read/write from all
nodes or the majority of nodes. The `WeaklyUp` node is not counted
as part of the cluster. So 3 nodes + 5 `WeaklyUp` is essentially a
3 node cluster as far as consistent actions are concerned.

This sample uses the replicated data type `GCounter` to implement a counter that can be written to on any node of the
cluster: 

Scala
:  @@snip [ReplicatorSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/ddata/typed/scaladsl/ReplicatorDocSpec.scala) { #sample }

Java
:  @@snip [ReplicatorTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/ddata/typed/javadsl/ReplicatorDocSample.java) { #sample }

Although you can interact with the `Replicator` using the @scala[`ActorRef[Replicator.Command]`]@java[`ActorRef<Replicator.Command>`]
from @scala[`DistributedData(ctx.system).replicator`]@java[`DistributedData(ctx.getSystem()).replicator()`] it's
often more convenient to use the `ReplicatorMessageAdapter` as in the above example.

<a id="replicator-update"></a>
### Update

To modify and replicate a data value you send a `Replicator.Update` message to the local
`Replicator`.

In the above example, for an incoming `Increment` command, we send the `replicator` a `Replicator.Update` request,
it contains five values:

 1. the @scala[`Key`]@java[`KEY`] we want to update
 1. the data to use as the empty state if the replicator has not seen the key before
 1. the @ref:[write consistency level](#write-consistency) we want for the update
 1. an @scala[`ActorRef[Replicator.UpdateResponse[GCounter]]`]@java[`ActorRef<Replicator.UpdateResponse<GCounter>>`] 
    to respond to when the update is completed
 1. a `modify` function that takes a previous state and updates it, in our case by incrementing it with 1

@@@ div { .group-scala }
There is alternative way of constructing the function for the `Update` message:

Scala
:  @@snip [ReplicatorSpec.scala](/akka-cluster-typed/src/test/scala/akka/cluster/ddata/typed/scaladsl/ReplicatorCompileOnlyTest.scala) { #curried-update }

@@@

The current data value for the `key` of the `Update` is passed as parameter to the `modify`
function of the `Update`. The function is supposed to return the new value of the data, which
will then be replicated according to the given @ref:[write consistency level](#write-consistency).

The `modify` function is called by the `Replicator` actor and must therefore be a pure
function that only uses the data parameter and stable fields from enclosing scope. It must
for example not access the `ActorContext` or mutable state of an enclosing actor.
`Update` is intended to only be sent from an actor running in same local `ActorSystem`
 as the `Replicator`, because the `modify` function is typically not serializable.

You will always see your own writes. For example if you send two `Update` messages
changing the value of the same `key`, the `modify` function of the second message will
see the change that was performed by the first `Update` message. 

As reply of the `Update` a `Replicator.UpdateSuccess` is sent to the `replyTo` of the
`Update` if the value was successfully replicated according to the supplied consistency
level within the supplied timeout. Otherwise a `Replicator.UpdateFailure` subclass is
sent back. Note that a `Replicator.UpdateTimeout` reply does not mean that the update completely failed
or was rolled back. It may still have been replicated to some nodes, and will eventually
be replicated to all nodes with the gossip protocol.

It is possible to abort the `Update` when inspecting the state parameter that is passed in to
the `modify` function by throwing an exception. That happens before the update is performed and
a `Replicator.ModifyFailure` is sent back as reply. 

### Get

To retrieve the current value of a data you send `Replicator.Get` message to the
`Replicator`. 

The example has the `GetValue` command, which is asking the replicator for current value. Note how the `replyTo` from the
incoming message can be used when the `GetSuccess` response from the replicator is received.

@@@ div { .group-scala }
Alternative way of constructing the function for the `Get` and `Delete`:

Scala
:  @@snip [ReplicatorSpec.scala](/akka-cluster-typed/src/test/scala/akka/cluster/ddata/typed/scaladsl/ReplicatorCompileOnlyTest.scala) { #curried-get }

@@@

For a `Get` you supply a @ref:[read consistency level](#read-consistency).

You will always read your own writes. For example if you send a `Update` message
followed by a `Get` of the same `key` the `Get` will retrieve the change that was
performed by the preceding `Update` message. However, the order of the reply messages are
not defined, i.e. in the previous example you may receive the `GetSuccess` before
the `UpdateSuccess`.

As reply of the `Get` a `Replicator.GetSuccess` is sent to the `replyTo` of the
`Get` if the value was successfully retrieved according to the supplied consistency
level within the supplied timeout. Otherwise a `Replicator.GetFailure` is sent.
If the key does not exist the reply will be `Replicator.NotFound`.

### Subscribe

Whenever the distributed counter in the example is updated, we cache the value so that we can answer
requests about the value without the extra interaction with the replicator using the `GetCachedValue` command.

When we start up the actor we subscribe it to changes for our key, meaning whenever the replicator observes a change
for the counter our actor will receive a @scala[`Replicator.Changed[GCounter]`]@java[`Replicator.Changed<GCounter>`]. Since
this is not a message in our protocol, we use a message transformation function to wrap it in the internal `InternalSubscribeResponse`
message, which is then handled in the regular message handling of the behavior, as shown in the above example.
Subscribers will be notified of changes, if there are any, based on the 
configurable `akka.cluster.distributed-data.notify-subscribers-interval`.

The subscriber is automatically unsubscribed if the subscriber is terminated. A subscriber can
also be de-registered with the `replicatorAdapter.unsubscribe(key)` function.

In addition to subscribing to individual keys it is possible to subscribe to all keys with a given prefix
by using a `*` at the end of the key `id`. For example `GCounterKey("counter-*")`. Notifications will be
sent for all matching keys, also new keys added later.

### Delete

A data entry can be deleted by sending a `Replicator.Delete` message to the local
`Replicator`. As reply of the `Delete` a `Replicator.DeleteSuccess` is sent to
the `replyTo` of the `Delete` if the value was successfully deleted according to the supplied
consistency level within the supplied timeout. Otherwise a `Replicator.ReplicationDeleteFailure`
is sent. Note that `ReplicationDeleteFailure` does not mean that the delete completely failed or
was rolled back. It may still have been replicated to some nodes, and may eventually be replicated
to all nodes.

A deleted key cannot be reused again, but it is still recommended to delete unused
data entries because that reduces the replication overhead when new nodes join the cluster.
Subsequent `Delete`, `Update` and `Get` requests will be replied with `Replicator.DataDeleted`.
Subscribers will receive `Replicator.Deleted`.

The @ref:[automatic expiry](#expire) is an alternative for removing unused data entries.
 
@@@ warning

As deleted keys continue to be included in the stored data on each node as well as in gossip
messages, a continuous series of updates and deletes of top-level entities will result in
growing memory usage until an ActorSystem runs out of memory. To use Akka Distributed Data
where frequent adds and removes are required, you should use @ref:[automatic expiry](#expire) 
or a fixed number of top-level data types that support both updates and removals, for example `ORMap` or `ORSet`.

@@@

### Expire

A data entry can automatically be removed after a period of inactivity, i.e. when there has been no access of
the entry with `Get`, `Update` or `Delete`.

Expiry is enabled for configured keys:

```
akka.cluster.distributed-data.expire-keys-after-inactivity {
 "key-1" = 10 minutes
 "cache-*" = 2 minutes
}
```

Prefix matching is supported by using `*` at the end of a key.

Expiry can be enabled for all entries by specifying:

```
akka.cluster.distributed-data.expire-keys-after-inactivity {
  "*" = 10 minutes
}
```

Subscribers will receive `Replicator.Expired` when an entry has expired.

Expired entries are completely removed and does not leave any tombstones as is the case for @ref:[Delete](#delete). 
Expired keys can be reused again. Also @ref:[deleted](#delete) entries can be expired and then completely removed.

### Consistency

The consistency level that is supplied in the @ref:[Update](#update) and @ref:[Get](#get)
specifies per request how many replicas that must respond successfully to a write and read request.

`WriteAll` and `ReadAll` is the strongest consistency level, but also the slowest and with
lowest availability. For example, it is enough that one node is unavailable for a `Get` request
and you will not receive the value.

For low latency reads you use @scala[`ReadLocal`]@java[`readLocal`] with the risk of retrieving stale data, i.e. updates
from other nodes might not be visible yet.

#### Write consistency

When using @scala[`WriteLocal`]@java[`writeLocal`] the `Update` is only written to the local replica and then disseminated
in the background with the gossip protocol, which can take few seconds to spread to all nodes.

For an update you supply a write consistency level which has the following meaning:

 * @scala[`WriteLocal`]@java[`writeLocal`] the value will immediately only be written to the local replica,
and later disseminated with gossip
 * `WriteTo(n)` the value will immediately be written to at least `n` replicas,
including the local replica
 * `WriteMajority` the value will immediately be written to a majority of replicas, i.e.
at least **N/2 + 1** replicas, where N is the number of nodes in the cluster
(or cluster role group)
 * `WriteMajorityPlus` is like `WriteMajority` but with the given number of `additional` nodes added
   to the majority count. At most all nodes. This gives better tolerance for membership changes between
   writes and reads. Exiting nodes are excluded using `WriteMajorityPlus` because those are typically about to be removed
   and will not be able to respond.
 * `WriteAll` the value will immediately be written to all nodes in the cluster (or all nodes in the cluster role group).
   Exiting nodes are excluded using `WriteAll` because those are typically about to be removed and will not be able to respond.

When you specify to write to `n` out of `x`  nodes, the update will first replicate to `n` nodes.
If there are not enough Acks after a 1/5th of the timeout, the update will be replicated to `n` other
nodes. If there are less than n nodes left all of the remaining nodes are used. Reachable nodes
are preferred over unreachable nodes.

Note that `WriteMajority` and `WriteMajorityPlus` have a `minCap` parameter that is useful to specify to
achieve better safety for small clusters.

#### Read consistency

If consistency is a priority, you can ensure that a read always reflects the most recent
write by using the following formula:

```
(nodes_written + nodes_read) > N
```

where N is the total number of nodes in the cluster, or the number of nodes with the role that is
used for the `Replicator`.

You supply a consistency level which has the following meaning:

 * @scala[`ReadLocal`]@java[`readLocal`] the value will only be read from the local replica
 * `ReadFrom(n)` the value will be read and merged from `n` replicas,
including the local replica
 * `ReadMajority` the value will be read and merged from a majority of replicas, i.e.
at least **N/2 + 1** replicas, where N is the number of nodes in the cluster
(or cluster role group)
* `ReadMajorityPlus` is like `ReadMajority` but with the given number of `additional` nodes added
   to the majority count. At most all nodes. This gives better tolerance for membership changes between
   writes and reads. Exiting nodes are excluded using `ReadMajorityPlus` because those are typically about to be
  removed and will not be able to respond.
 * `ReadAll` the value will be read and merged from all nodes in the cluster (or all nodes in the cluster role group).
   Exiting nodes are excluded using `ReadAll` because those are typically about to be removed and will not be able to respond.

Note that `ReadMajority` and `ReadMajorityPlus` have a `minCap` parameter that is useful to specify to achieve
better safety for small clusters.  Additionally, reads from remote replicas are incorporated into the local
replica's state.

#### Consistency and response types

When using `ReadLocal`, you will never receive a `GetFailure` response, since the local replica is always available to
local readers. `WriteLocal` however may still reply with `UpdateFailure` messages if the `modify` function
throws an exception, or if it fails to persist to @ref:[durable storage](#durable-storage).

#### Examples

In a 7 node cluster these consistency properties are achieved by writing to 4 nodes
and reading from 4 nodes, or writing to 5 nodes and reading from 3 nodes.

By combining `WriteMajority` and `ReadMajority` levels a read always reflects the most recent write.
The `Replicator` writes and reads to a majority of replicas, i.e. **N / 2 + 1**. For example,
in a 5 node cluster it writes to 3 nodes and reads from 3 nodes. In a 6 node cluster it writes
to 4 nodes and reads from 4 nodes.

You can define a minimum number of nodes for `WriteMajority` and `ReadMajority`,
this will minimize the risk of reading stale data. Minimum cap is
provided by minCap property of `WriteMajority` and `ReadMajority` and defines the required majority.
If the minCap is higher then **N / 2 + 1** the minCap will be used.

For example if the minCap is 5 the `WriteMajority` and `ReadMajority` for cluster of 3 nodes will be 3, for
cluster of 6 nodes will be 5 and for cluster of 12 nodes will be 7 ( **N / 2 + 1** ).

For small clusters (<7) the risk of membership changes between a WriteMajority and ReadMajority
is rather high and then the nice properties of combining majority write and reads are not
guaranteed. Therefore the `ReadMajority` and `WriteMajority` have a `minCap` parameter that
is useful to specify to achieve better safety for small clusters. It means that if the cluster
size is smaller than the majority size it will use the `minCap` number of nodes but at most
the total size of the cluster.

In some rare cases, when performing an `Update` it is needed to first try to fetch latest data from
other nodes. That can be done by first sending a `Get` with `ReadMajority` and then continue with
the `Update` when the `GetSuccess`, `GetFailure` or `NotFound` reply is received. This might be
needed when you need to base a decision on latest information or when removing entries from an `ORSet`
or `ORMap`. If an entry is added to an `ORSet` or `ORMap` from one node and removed from another
node the entry will only be removed if the added entry is visible on the node where the removal is
performed (hence the name observed-removed set).

@@@ warning

*Caveat:* Even if you use `WriteMajority` and `ReadMajority` there is small risk that you may
read stale data if the cluster membership has changed between the `Update` and the `Get`.
For example, in cluster of 5 nodes when you `Update` and that change is written to 3 nodes:
n1, n2, n3. Then 2 more nodes are added and a `Get` request is reading from 4 nodes, which
happens to be n4, n5, n6, n7, i.e. the value on n1, n2, n3 is not seen in the response of the
`Get` request. For additional tolerance of membership changes between writes and reads you can
use `WriteMajorityPlus` and `ReadMajorityPlus`.

@@@

### Running separate instances of the replicator

For some use cases, for example when limiting the replicator to certain roles, or using different subsets on different roles,
it makes sense to start separate replicators, this needs to be done on all nodes, or 
the group of nodes tagged with a specific role. To do this with Distributed Data you will first
have to start a classic `Replicator` and pass it to the `Replicator.behavior` method that takes a classic
actor ref. All such `Replicator`s must run on the same path in the classic actor hierarchy.

A standalone `ReplicatorMessageAdapter` can also be created for a given `Replicator` instead of creating
one via the `DistributedData` extension.

## Replicated data types

Akka contains a set of useful replicated data types and it is fully possible to implement custom replicated data types. 

The data types must be convergent (stateful) CRDTs and implement the @scala[`ReplicatedData` trait]@java[`AbstractReplicatedData` interface],
i.e. they provide a monotonic merge function and the state changes always converge.

You can use your own custom @scala[`ReplicatedData` or `DeltaReplicatedData`]@java[`AbstractReplicatedData` or `AbstractDeltaReplicatedData`] types, and several types are provided
by this package, such as:

 * Counters: `GCounter`, `PNCounter`
 * Sets: `GSet`, `ORSet`
 * Maps: `ORMap`, `ORMultiMap`, `LWWMap`, `PNCounterMap`
 * Registers: `LWWRegister`, `Flag`
 
### Counters

`GCounter` is a "grow only counter". It only supports increments, no decrements.

It works in a similar way as a vector clock. It keeps track of one counter per node and the total
value is the sum of these counters. The `merge` is implemented by taking the maximum count for
each node.

If you need both increments and decrements you can use the `PNCounter` (positive/negative counter).

It is tracking the increments (P) separate from the decrements (N). Both P and N are represented
as two internal `GCounter`s. Merge is handled by merging the internal P and N counters.
The value of the counter is the value of the P counter minus the value of the N counter.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #pncounter }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #pncounter }

`GCounter` and `PNCounter` have support for @ref:[delta-CRDT](#delta-crdt) and don't need causal
delivery of deltas.

Several related counters can be managed in a map with the `PNCounterMap` data type.
When the counters are placed in a `PNCounterMap` as opposed to placing them as separate top level
values they are guaranteed to be replicated together as one unit, which is sometimes necessary for
related data.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #pncountermap }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #pncountermap }

### Sets

If you only need to add elements to a set and not remove elements the `GSet` (grow-only set) is
the data type to use. The elements can be any type of values that can be serialized.
Merge is the union of the two sets.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #gset }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #gset }

`GSet` has support for @ref:[delta-CRDT](#delta-crdt) and it doesn't require causal delivery of deltas.

If you need add and remove operations you should use the `ORSet` (observed-remove set).
Elements can be added and removed any number of times. If an element is concurrently added and
removed, the add will win. You cannot remove an element that you have not seen.

The `ORSet` has a version vector that is incremented when an element is added to the set.
The version for the node that added the element is also tracked for each element in a so
called "birth dot". The version vector and the dots are used by the `merge` function to
track causality of the operations and resolve concurrent updates.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #orset }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #orset }

`ORSet` has support for @ref:[delta-CRDT](#delta-crdt) and it requires causal delivery of deltas.

### Maps

`ORMap` (observed-remove map) is a map with keys of `Any` type and the values are `ReplicatedData`
types themselves. It supports add, update and remove any number of times for a map entry.

If an entry is concurrently added and removed, the add will win. You cannot remove an entry that
you have not seen. This is the same semantics as for the `ORSet`.

If an entry is concurrently updated to different values the values will be merged, hence the
requirement that the values must be `ReplicatedData` types.

While the `ORMap` supports removing and re-adding keys any number of times, the impact that this has on the values
can be non-deterministic. A merge will always attempt to merge two values for the same key, regardless of whether that
key has been removed and re-added in the meantime, an attempt to replace a value with a new one may not have the
intended effect. This means that old values can effectively be resurrected if a node, that has seen both the remove and
the update,gossips with a node that has seen neither. One consequence of this is that changing the value type of the
CRDT, for example, from a `GCounter` to a `GSet`, could result in the merge function for the CRDT always failing.
This could be an unrecoverable state for the node, hence, the types of `ORMap` values must never change for a given key.

It is rather inconvenient to use the `ORMap` directly since it does not expose specific types
of the values. The `ORMap` is intended as a low level tool for building more specific maps,
such as the following specialized maps.

`ORMultiMap` (observed-remove multi-map) is a multi-map implementation that wraps an
`ORMap` with an `ORSet` for the map's value.

`PNCounterMap` (positive negative counter map) is a map of named counters (where the name can be of any type).
It is a specialized `ORMap` with `PNCounter` values.

`LWWMap` (last writer wins map) is a specialized `ORMap` with `LWWRegister` (last writer wins register)
values.

`ORMap`, `ORMultiMap`, `PNCounterMap` and `LWWMap` have support for @ref:[delta-CRDT](#delta-crdt) and they require causal
delivery of deltas. Support for deltas here means that the `ORSet` being underlying key type for all those maps
uses delta propagation to deliver updates. Effectively, the update for map is then a pair, consisting of delta for the `ORSet`
being the key and full update for the respective value (`ORSet`, `PNCounter` or `LWWRegister`) kept in the map.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #ormultimap }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #ormultimap }

When a data entry is changed the full state of that entry is replicated to other nodes, i.e.
when you update a map, the whole map is replicated. Therefore, instead of using one `ORMap`
with 1000 elements it is more efficient to split that up in 10 top level `ORMap` entries
with 100 elements each. Top level entries are replicated individually, which has the
trade-off that different entries may not be replicated at the same time and you may see
inconsistencies between related entries. Separate top level entries cannot be updated atomically
together.

There is a special version of `ORMultiMap`, created by using separate constructor
`ORMultiMap.emptyWithValueDeltas[A, B]`, that also propagates the updates to its values (of `ORSet` type) as deltas.
This means that the `ORMultiMap` initiated with `ORMultiMap.emptyWithValueDeltas` propagates its updates as pairs
consisting of delta of the key and delta of the value. It is much more efficient in terms of network bandwidth consumed.

However, this behavior has not been made default for `ORMultiMap` and if you wish to use it in your code, you
need to replace invocations of `ORMultiMap.empty[A, B]` (or `ORMultiMap()`) with `ORMultiMap.emptyWithValueDeltas[A, B]`
where `A` and `B` are types respectively of keys and values in the map.

Please also note, that despite having the same Scala type, `ORMultiMap.emptyWithValueDeltas`
is not compatible with 'vanilla' `ORMultiMap`, because of different replication mechanism.
One needs to be extra careful not to mix the two, as they have the same
type, so compiler will not hint the error.
Nonetheless `ORMultiMap.emptyWithValueDeltas` uses the same `ORMultiMapKey` type as the
'vanilla' `ORMultiMap` for referencing.

Note that `LWWRegister` and therefore `LWWMap` relies on synchronized clocks and should only be used
when the choice of value is not important for concurrent updates occurring within the clock skew. Read more
in the below section about `LWWRegister`.

### Flags and Registers

`Flag` is a data type for a boolean value that is initialized to `false` and can be switched
to `true`. Thereafter it cannot be changed. `true` wins over `false` in merge.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #flag }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #flag }

`LWWRegister` (last writer wins register) can hold any (serializable) value.

Merge of a `LWWRegister` takes the register with highest timestamp. Note that this
relies on synchronized clocks. *LWWRegister* should only be used when the choice of
value is not important for concurrent updates occurring within the clock skew.

Merge takes the register updated by the node with lowest address (`UniqueAddress` is ordered)
if the timestamps are exactly the same.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #lwwregister }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #lwwregister }

Instead of using timestamps based on `System.currentTimeMillis()` time it is possible to
use a timestamp value based on something else, for example an increasing version number
from a database record that is used for optimistic concurrency control.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #lwwregister-custom-clock }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #lwwregister-custom-clock }

For first-write-wins semantics you can use the `LWWRegister#reverseClock` instead of the
`LWWRegister#defaultClock`.

The `defaultClock` is using max value of `System.currentTimeMillis()` and `currentTimestamp + 1`.
This means that the timestamp is increased for changes on the same node that occurs within
the same millisecond. It also means that it is safe to use the `LWWRegister` without
synchronized clocks when there is only one active writer, e.g. a Cluster Singleton. Such a
single writer should then first read current value with `ReadMajority` (or more) before
changing and writing the value with `WriteMajority` (or more). When using `LWWRegister`
with Cluster Singleton it's also recommended to enable:

```
# Update and Get operations are sent to oldest nodes first.
akka.cluster.distributed-data.prefer-oldest = on
```

### Delta-CRDT

[Delta State Replicated Data Types](https://arxiv.org/abs/1603.01529)
are supported. A delta-CRDT is a way to reduce the need for sending the full state
for updates. For example adding element `'c'` and `'d'` to set `{'a', 'b'}` would
result in sending the delta `{'c', 'd'}` and merge that with the state on the
receiving side, resulting in set `{'a', 'b', 'c', 'd'}`.

The protocol for replicating the deltas supports causal consistency if the data type
is marked with `RequiresCausalDeliveryOfDeltas`. Otherwise it is only eventually
consistent. Without causal consistency it means that if elements `'c'` and `'d'` are
added in two separate *Update* operations these deltas may occasionally be propagated
to nodes in a different order to the causal order of the updates. For this example it
can result in that set `{'a', 'b', 'd'}` can be seen before element 'c' is seen. Eventually
it will be `{'a', 'b', 'c', 'd'}`.

Note that the full state is occasionally also replicated for delta-CRDTs, for example when
new nodes are added to the cluster or when deltas could not be propagated because
of network partitions or similar problems.

The the delta propagation can be disabled with configuration property:

```
akka.cluster.distributed-data.delta-crdt.enabled=off
```

### Custom Data Type

You can implement your own data types. The only requirement is that it implements
the @scala[`merge`]@java[`mergeData`] function of the @scala[`ReplicatedData`]@java[`AbstractReplicatedData`] trait.

A nice property of stateful CRDTs is that they typically compose nicely, i.e. you can combine several
smaller data types to build richer data structures. For example, the `PNCounter` is composed of
two internal `GCounter` instances to keep track of increments and decrements separately.

Here is s simple implementation of a custom `TwoPhaseSet` that is using two internal `GSet` types
to keep track of addition and removals.  A `TwoPhaseSet` is a set where an element may be added and
removed, but never added again thereafter.

Scala
: @@snip [TwoPhaseSet.scala](/akka-docs/src/test/scala/docs/ddata/TwoPhaseSet.scala) { #twophaseset }

Java
: @@snip [TwoPhaseSet.java](/akka-docs/src/test/java/jdocs/ddata/TwoPhaseSet.java) { #twophaseset }

Data types should be immutable, i.e. "modifying" methods should return a new instance.

Implement the additional methods of @scala[`DeltaReplicatedData`]@java[`AbstractDeltaReplicatedData`] if it has support for delta-CRDT replication.
 
#### Serialization

The data types must be serializable with an @ref:[Akka Serializer](../serialization.md).
It is highly recommended that you implement  efficient serialization with Protobuf or similar
for your custom data types. The built in data types are marked with `ReplicatedDataSerialization`
and serialized with `akka.cluster.ddata.protobuf.ReplicatedDataSerializer`.

Serialization of the data types are used in remote messages and also for creating message
digests (SHA-1) to detect changes. Therefore it is important that the serialization is efficient
and produce the same bytes for the same content. For example sets and maps should be sorted
deterministically in the serialization.

This is a protobuf representation of the above `TwoPhaseSet`:

@@snip [TwoPhaseSetMessages.proto](/akka-docs/src/test/../main/protobuf/TwoPhaseSetMessages.proto) { #twophaseset }

The serializer for the `TwoPhaseSet`:

Scala
: @@snip [TwoPhaseSetSerializer.scala](/akka-docs/src/test/scala/docs/ddata/protobuf/TwoPhaseSetSerializer.scala) { #serializer }

Java
: @@snip [TwoPhaseSetSerializer.java](/akka-docs/src/test/java/jdocs/ddata/protobuf/TwoPhaseSetSerializer.java) { #serializer }

Note that the elements of the sets are sorted so the SHA-1 digests are the same for the same elements.

You register the serializer in configuration:

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #serializer-config }

Java
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #japi-serializer-config }

Using compression can sometimes be a good idea to reduce the data size. Gzip compression is
provided by the @scala[`akka.cluster.ddata.protobuf.SerializationSupport` trait]@java[`akka.cluster.ddata.protobuf.AbstractSerializationSupport` interface]:

Scala
: @@snip [TwoPhaseSetSerializer.scala](/akka-docs/src/test/scala/docs/ddata/protobuf/TwoPhaseSetSerializer.scala) { #compression }

Java
: @@snip [TwoPhaseSetSerializerWithCompression.java](/akka-docs/src/test/java/jdocs/ddata/protobuf/TwoPhaseSetSerializerWithCompression.java) { #compression }

The two embedded `GSet` can be serialized as illustrated above, but in general when composing
new data types from the existing built in types it is better to make use of the existing
serializer for those types. This can be done by declaring those as bytes fields in protobuf:

@@snip [TwoPhaseSetMessages.proto](/akka-docs/src/test/../main/protobuf/TwoPhaseSetMessages.proto) { #twophaseset2 }

and use the methods `otherMessageToProto` and `otherMessageFromBinary` that are provided
by the `SerializationSupport` trait to serialize and deserialize the `GSet` instances. This
works with any type that has a registered Akka serializer. This is how such an serializer would
look like for the `TwoPhaseSet`:

Scala
: @@snip [TwoPhaseSetSerializer2.scala](/akka-docs/src/test/scala/docs/ddata/protobuf/TwoPhaseSetSerializer2.scala) { #serializer }

Java
: @@snip [TwoPhaseSetSerializer2.java](/akka-docs/src/test/java/jdocs/ddata/protobuf/TwoPhaseSetSerializer2.java) { #serializer }


## Durable Storage

By default the data is only kept in memory. It is redundant since it is replicated to other nodes
in the cluster, but if you stop all nodes the data is lost, unless you have saved it
elsewhere.

Entries can be configured to be durable, i.e. stored on local disk on each node. The stored data will be loaded
next time the replicator is started, i.e. when actor system is restarted. This means data will survive as
long as at least one node from the old cluster takes part in a new cluster. The keys of the durable entries
are configured with:

```
akka.cluster.distributed-data.durable.keys = ["a", "b", "durable*"]
```

Prefix matching is supported by using `*` at the end of a key.

All entries can be made durable by specifying:

```
akka.cluster.distributed-data.durable.keys = ["*"]
```

@scala[[LMDB](https://symas.com/lmdb/technical/)]@java[[LMDB](https://github.com/lmdbjava/lmdbjava/)] is the default storage implementation. It is
possible to replace that with another implementation by implementing the actor protocol described in
`akka.cluster.ddata.DurableStore` and defining the `akka.cluster.distributed-data.durable.store-actor-class`
property for the new implementation.

@@@ note { title="Java 17" }

When using LMDB with Java 17 you have to add JVM flags `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`.

@@@

The location of the files for the data is configured with:

Scala
:   ```
# Directory of LMDB file. There are two options:
# 1. A relative or absolute path to a directory that ends with 'ddata'
#    the full name of the directory will contain name of the ActorSystem
#    and its remote port.
# 2. Otherwise the path is used as is, as a relative or absolute path to
#    a directory.
akka.cluster.distributed-data.durable.lmdb.dir = "ddata"
```

Java
:   ```
# Directory of LMDB file. There are two options:
# 1. A relative or absolute path to a directory that ends with 'ddata'
#    the full name of the directory will contain name of the ActorSystem
#    and its remote port.
# 2. Otherwise the path is used as is, as a relative or absolute path to
#    a directory.
akka.cluster.distributed-data.durable.lmdb.dir = "ddata"
```


When running in production you may want to configure the directory to a specific
path (alt 2), since the default directory contains the remote port of the
actor system to make the name unique. If using a dynamically assigned
port (0) it will be different each time and the previously stored data
will not be loaded.

Making the data durable has a performance cost. By default, each update is flushed
to disk before the `UpdateSuccess` reply is sent. For better performance, but with the risk of losing
the last writes if the JVM crashes, you can enable write behind mode. Changes are then accumulated during
a time period before it is written to LMDB and flushed to disk. Enabling write behind is especially
efficient when performing many writes to the same key, because it is only the last value for each key
that will be serialized and stored. The risk of losing writes if the JVM crashes is small since the
data is typically replicated to other nodes immediately according to the given `WriteConsistency`.

```
akka.cluster.distributed-data.durable.lmdb.write-behind-interval = 200 ms
```

Note that you should be prepared to receive `WriteFailure` as reply to an `Update` of a
durable entry if the data could not be stored for some reason. When enabling `write-behind-interval`
such errors will only be logged and `UpdateSuccess` will still be the reply to the `Update`.

There is one important caveat when it comes pruning of @ref:[CRDT Garbage](#crdt-garbage) for durable data.
If an old data entry that was never pruned is injected and merged with existing data after
that the pruning markers have been removed the value will not be correct. The time-to-live
of the markers is defined by configuration
`akka.cluster.distributed-data.durable.pruning-marker-time-to-live` and is in the magnitude of days.
This would be possible if a node with durable data didn't participate in the pruning
(e.g. it was shutdown) and later started after this time. A node with durable data should not
be stopped for longer time than this duration and if it is joining again after this
duration its data should first be manually removed (from the lmdb directory).

## Limitations

There are some limitations that you should be aware of.

CRDTs cannot be used for all types of problems, and eventual consistency does not fit
all domains. Sometimes you need strong consistency.

It is not intended for *Big Data*. The number of top level entries should not exceed 100000.
When a new node is added to the cluster all these entries are transferred (gossiped) to the
new node. The entries are split up in chunks and all existing nodes collaborate in the gossip,
but it will take a while (tens of seconds) to transfer all entries and this means that you
cannot have too many top level entries. The current recommended limit is 100000. We will
be able to improve this if needed, but the design is still not intended for billions of entries.

All data is held in memory, which is another reason why it is not intended for *Big Data*.

When a data entry is changed the full state of that entry may be replicated to other nodes
if it doesn't support @ref:[delta-CRDT](#delta-crdt). The full state is also replicated for delta-CRDTs,
for example when new nodes are added to the cluster or when deltas could not be propagated because
of network partitions or similar problems. This means that you cannot have too large
data entries, because then the remote message size will be too large.

### CRDT Garbage

One thing that can be problematic with CRDTs is that some data types accumulate history (garbage).
For example a `GCounter` keeps track of one counter per node. If a `GCounter` has been updated
from one node it will associate the identifier of that node forever. That can become a problem
for long running systems with many cluster nodes being added and removed. To solve this problem
the `Replicator` performs pruning of data associated with nodes that have been removed from the
cluster. Data types that need pruning have to implement the `RemovedNodePruning` trait. See the
API documentation of the `Replicator` for details.

## Learn More about CRDTs

 * [Strong Eventual Consistency and Conflict-free Replicated Data Types (video)](https://www.youtube.com/watch?v=oyUHd894w18&amp;feature=youtu.be)
talk by Mark Shapiro
 * [A comprehensive study of Convergent and Commutative Replicated Data Types](https://hal.inria.fr/file/index/docid/555588/filename/techreport.pdf)
paper by Mark Shapiro et. al.

## Configuration

The `DistributedData` extension can be configured with the following properties:

@@snip [reference.conf](/akka-distributed-data/src/main/resources/reference.conf) { #distributed-data }

## Example project

@java[[Distributed Data example project](../attachments/akka-sample-distributed-data-java.zip)]
@scala[[Distributed Data example project](../attachments/akka-sample-distributed-data-scala.zip)]
is an example project that can be downloaded, and with instructions of how to run.

This project contains several samples illustrating how to use Distributed Data.
