# Sharded Daemon Process

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Sharded Daemon Process, you must add the following dependency in your project:

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

Sharded Daemon Process provides a way to run `N` actors, each given a numeric id starting from `0` that are then kept alive
and balanced across the cluster. When a rebalance is needed the actor is stopped and, triggered by a keep alive from
a Cluster Singleton (the keep alive should be seen as an implementation detail and may change in future versions).

The intended use case is for splitting data processing workloads across a set number of workers that each get to work on a subset
of the data that needs to be processed. This is commonly needed to create projections based on the event streams available
from all the @ref:[EventSourcedBehaviors](persistence.md) in a CQRS application. Events are tagged with one out of `N` tags
used to split the workload of consuming and updating a projection between `N` workers.

For cases where a single actor needs to be kept alive see @ref:[Cluster Singleton](cluster-singleton.md)

## Basic example

To set up a set of actors running with Sharded Daemon process each node in the cluster needs to run the same initialization
when starting up:

Scala
:  @@snip [ShardedDaemonProcessExample.scala](/akka-cluster-sharding-typed/src/test/scala/akka/cluster/sharding/typed/scaladsl/ShardedDaemonProcessSpec.scala) { #tag-processing }

Java
:  @@snip [ShardedDaemonProcessExample.java](/akka-cluster-sharding-typed/src/test/java/akka/cluster/sharding/typed/javadsl/ShardedDaemonProcessCompileOnlyTest.java) { #tag-processing }

An additional factory method is provided for further configurability and providing a graceful stop message for the actor.

## Addressing the actors

In use cases where you need to send messages to the daemon process actors it is recommended to use the @ref:[system receptionist](actor-discovery.md)
either with a single `ServiceKey` which all daemon process actors register themeselves to for broadcasts or individual keys if more fine grained messaging is needed.

## Dynamic scaling of number of workers

Starting the sharded daemon process with `initWithContext` returns an `ActorRef[ShardedDaemonProcessCommand]` that accepts a @apidoc[ChangeNumberOfProcesses] command to rescale the process to a new number of workers.

The rescaling process among other things includes the process actors stopping themselves in response to a stop message 
so may be a relatively slow operation. If a subsequent request to rescale is sent while one is in progress it is responded
to with a failure response.

A rolling upgrade switching from a static number of workers to a dynamic number is possible. 
It is not safe to do a rolling upgrade from dynamic number of workers to static without a full cluster shutdown.

### Colocate processes

When using the default shard allocation strategy the processes for different names are allocated independent of
each other, i.e. the same process index for different process names may be allocated to different nodes.
Colocating processes can be useful to share resources, such as Projections with @ref:[EventsBySliceFirehoseQuery](../persistence-query.md#eventsbyslice-and-currenteventsbyslice)

To colocate such processes you can use the @apidoc[akka.cluster.sharding.ConsistentHashingShardAllocationStrategy]
as `shardAllocationStrategy` parameter of the `init` or `initWithContext` methods. 

@@@ note
Create a new instance of the `ConsistentHashingShardAllocationStrategy` for each Sharded Daemon Process name, i.e. a `ConsistentHashingShardAllocationStrategy` instance must not be shared.
@@@

The shard identifier that is used by Sharded Daemon Process is the same as the process index, i.e. processes with
the same index will be colocated.

The allocation strategy is using [Consistent Hashing](https://tom-e-white.com/2007/11/consistent-hashing.html)
of the Cluster membership ring to assign a shard to a node. When adding or removing nodes it will rebalance
according to the new consistent hashing, but that means that only a few shards will be rebalanced and others
remain on the same location. When there are changes to the Cluster membership the shards may be on different
nodes for a while, but eventually, when the membership is stable, the shards with the same identifier will
end up on the same node.

## Scalability  

This cluster tool is intended for up to thousands of processes. Running with larger sets of processes might cause problems
with Akka Distributed Data replication or process keepalive messages.

## Configuration

The following configuration properties are read by the @apidoc[ShardedDaemonProcessSettings]
when created with a @apidoc[ActorSystem](typed.ActorSystem) parameter:

@@snip [reference.conf](/akka-cluster-sharding-typed/src/main/resources/reference.conf) { #sharded-daemon-process }
