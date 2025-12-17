# Split Brain Resolver

When operating an Akka cluster you must consider how to handle
[network partitions](https://en.wikipedia.org/wiki/Network_partition) (a.k.a. split brain scenarios)
and machine crashes (including JVM and hardware failures). This is crucial for correct behavior if
you use @ref:[Cluster Singleton](typed/cluster-singleton.md) or @ref:[Cluster Sharding](typed/cluster-sharding.md),
especially together with Akka Persistence.

The [Split Brain Resolver video](https://akka.io/blog/news/2020/06/08/akka-split-brain-resolver-video)
is a good starting point for learning why it is important to use a correct downing provider and
how the Split Brain Resolver works.

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Split Brain Resolver is part of `akka-cluster` and you probably already have that
dependency included. Otherwise, add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster_$scala.binary.version$
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster" }

## Enable the Split Brain Resolver

You need to enable the Split Brain Resolver by configuring it as downing provider in the configuration of
the `ActorSystem` (`application.conf`):

```
akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
```

You should also consider the different available @ref:[downing strategies](#strategies).

## The Problem

A fundamental problem in distributed systems is that network partitions (split brain scenarios) and
machine crashes are indistinguishable for the observer, i.e. a node can observe that there is a problem
with another node, but it cannot tell if it has crashed and will never be available again or if there is
a network issue that might or might not heal again after a while. Temporary and permanent failures are
indistinguishable because decisions must be made in finite time, and there always exists a temporary
failure that lasts longer than the time limit for the decision.

A third type of problem is if a process is unresponsive, e.g. because of overload, CPU starvation or
long garbage collection pauses. This is also indistinguishable from network partitions and crashes.
The only signal we have for decision is "no reply in given time for heartbeats" and this means that
phenomena causing delays or lost heartbeats are indistinguishable from each other and must be
handled in the same way.

When there is a crash, we would like to remove the affected node immediately from the cluster membership.
When there is a network partition or unresponsive process we would like to wait for a while in the hope
that it is a transient problem that will heal again, but at some point, we must give up and continue with
the nodes on one side of the partition and shut down nodes on the other side. Also, certain features are
not fully available during partitions so it might not matter that the partition is transient or not if
it just takes too long. Those two goals are in conflict with each other and there is a trade-off
between how quickly we can remove a crashed node and premature action on transient network partitions.

This is a difficult problem to solve given that the nodes on the different sides of the network partition
cannot communicate with each other. We must ensure that both sides can make this decision by themselves and
that they take the same decision about which part will keep running and which part will shut itself down.

Another type of problem that makes it difficult to see the "right" picture is when some nodes are not fully
connected and cannot communicate directly to each other but information can be disseminated between them via
other nodes.

The Akka cluster has a failure detector that will notice network partitions and machine crashes (but it
cannot distinguish the two). It uses periodic heartbeat messages to check if other nodes are available
and healthy. These observations by the failure detector are referred to as a node being *unreachable*
and it may become *reachable* again if the failure detector observes that it can communicate with it again.  

The failure detector in itself is not enough for making the right decision in all situations.
The naive approach is to remove an unreachable node from the cluster membership after a timeout.
This works great for crashes and short transient network partitions, but not for long network
partitions. Both sides of the network partition will see the other side as unreachable and
after a while remove it from its cluster membership. Since this happens on both sides the result
is that two separate disconnected clusters have been created.

If you use the timeout based auto-down feature in combination with Cluster Singleton or Cluster Sharding
that would mean that two singleton instances or two sharded entities with the same identifier would be running.
One would be running: one in each cluster.
For example when used together with Akka Persistence that could result in that two instances of a
persistent actor with the same `persistenceId` are running and writing concurrently to the
same stream of persistent events, which will have fatal consequences when replaying these events.

The default setting in Akka Cluster is to not remove unreachable nodes automatically and
the recommendation is that the decision of what to
do should be taken by a human operator or an external monitoring system. This is a valid solution,
but not very convenient if you do not have this staff or external system for other reasons.

If the unreachable nodes are not downed at all they will still be part of the cluster membership.
Meaning that Cluster Singleton and Cluster Sharding will not failover to another node. While there
are unreachable nodes new nodes that are joining the cluster will not be promoted to full worthy
members (with status Up). Similarly, leaving members will not be removed until all unreachable
nodes have been resolved. In other words, keeping unreachable members for an unbounded time is
undesirable.

With that introduction of the problem domain, it is time to look at the provided strategies for
handling network partition, unresponsive nodes and crashed nodes.

## Strategies

By default the @ref:[Keep Majority](#keep-majority) strategy will be used because it works well for
most systems. However, it's worth considering the other available strategies and pick a strategy that fits
the characteristics of your system. For example, in a Kubernetes environment the @ref:[Lease](#lease) strategy
can be a good choice.

Every strategy has a failure scenario where it makes a "wrong" decision. This section describes the different
strategies and guidelines of when to use what.

When there is uncertainty it selects to down more nodes than necessary, or even downing of all nodes.
Therefore Split Brain Resolver should always be combined with a mechanism to automatically start up nodes that
have been shutdown, and join them to the existing cluster or form a new cluster again.

You enable a strategy with the configuration property `akka.cluster.split-brain-resolver.active-strategy`.

### Stable after

All strategies are inactive until the cluster membership and the information about unreachable nodes
have been stable for a certain time period. Continuously adding more nodes while there is a network
partition does not influence this timeout, since the status of those nodes will not be changed to Up
while there are unreachable nodes. Joining nodes are not counted in the logic of the strategies. 

@@snip [reference.conf](/akka-cluster/src/main/resources/reference.conf) { #split-brain-resolver }

Set `akka.cluster.split-brain-resolver.stable-after` to a shorter duration to have quicker removal of crashed nodes,
at the price of risking too early action on transient network partitions that otherwise would have healed. Do not
set this to a shorter duration than the membership dissemination time in the cluster, which depends
on the cluster size. Recommended minimum duration for different cluster sizes:

|cluster size | stable-after|
|-------------|-------------|
|5    | 7 s |
|10   | 10 s|
|20   | 13 s|
|50   | 17 s|
|100  | 20 s|
|1000 | 30 s|

The different strategies may have additional settings that are described below.

@@@ note

It is important that you use the same configuration on all nodes.

@@@

The side of the split that decides to shut itself down will use the cluster *down* command
to initiate the removal of a cluster member. When that has been spread among the reachable nodes
it will be removed from the cluster membership.

It's good to terminate the `ActorSystem` and exit the JVM when the node is removed from the cluster.

That is handled by @ref:[Coordinated Shutdown](coordinated-shutdown.md)
but to exit the JVM it's recommended that you enable:

```
akka.coordinated-shutdown.exit-jvm = on
```

@@@ note

Some legacy containers may block calls to System.exit(..) and you may have to find an alternate
way to shut the app down. For example, when running Akka on top of a Spring / Tomcat setup, you
could replace the call to `System.exit(..)` with a call to Spring's ApplicationContext .close() method
(or with a HTTP call to Tomcat Manager's API to un-deploy the app).

@@@

### Keep Majority

The strategy named `keep-majority` will down the unreachable nodes if the current node is in
the majority part based on the last known membership information. Otherwise down the reachable nodes,
i.e. the own part. If the parts are of equal size the part containing the node with the lowest
address is kept.

This strategy is a good choice when the number of nodes in the cluster change dynamically and you can
therefore not use `static-quorum`.

This strategy also handles the edge case that may occur when there are membership changes at the same
time as the network partition occurs. For example, the status of two members are changed to `Up`
on one side but that information is not disseminated to the other side before the connection is broken.
Then one side sees two more nodes and both sides might consider themselves having a majority. It will
detect this situation and make the safe decision to down all nodes on the side that could be in minority
if the joining nodes were changed to `Up` on the other side. Note that this has the drawback that
if the joining nodes were not changed to `Up` and becoming a majority on the other side then each part
will shut down itself, terminating the whole cluster.

Note that if there are more than two partitions and none is in majority each part will shut down
itself, terminating the whole cluster.

If more than half of the nodes crash at the same time the other running nodes will down themselves
because they think that they are not in majority, and thereby the whole cluster is terminated.  

The decision can be based on nodes with a configured `role` instead of all nodes in the cluster.
This can be useful when some types of nodes are more valuable than others. You might for example
have some nodes responsible for persistent data and some nodes with stateless worker services.
Then it probably more important to keep as many persistent data nodes as possible even though
it means shutting down more worker nodes.

Configuration:

```
akka.cluster.split-brain-resolver.active-strategy=keep-majority
```

@@snip [reference.conf](/akka-cluster/src/main/resources/reference.conf) { #keep-majority }

### Static Quorum

The strategy named `static-quorum` will down the unreachable nodes if the number of remaining
nodes are greater than or equal to a configured `quorum-size`. Otherwise, it will down the reachable nodes,
i.e. it will shut down that side of the partition. In other words, the `quorum-size` defines the minimum
number of nodes that the cluster must have to be operational.

This strategy is a good choice when you have a fixed number of nodes in the cluster, or when you can
define a fixed number of nodes with a certain role.

For example, in a 9 node cluster you will configure the `quorum-size` to 5. If there is a network split
of 4 and 5 nodes the side with 5 nodes will survive and the other 4 nodes will be downed. After that,
in the 5 node cluster, no more failures can be handled, because the remaining cluster size would be
less than 5. In the case of another failure in that 5 node cluster all nodes will be downed.

Therefore, it is important that you join new nodes when old nodes have been removed.

Another consequence of this is that if there are unreachable nodes when starting up the cluster,
before reaching this limit, the cluster may shut itself down immediately. This is not an issue
if you start all nodes at approximately the same time or use the `akka.cluster.min-nr-of-members`
to define required number of members before the leader changes member status of 'Joining' members to 'Up'.
You can tune the timeout after which downing decisions are made using the `stable-after` setting.

You should not add more members to the cluster than **quorum-size * 2 - 1**. A warning is logged
if this recommendation is violated. If the exceeded cluster size remains when an SBR decision is
needed it will down all nodes because otherwise there is a risk that both sides may down each
other and thereby form two separate clusters.

For rolling updates, it's best to leave the cluster gracefully via
@ref:[Coordinated Shutdown](coordinated-shutdown.md) (SIGTERM).
For successful leaving, SBR will not be used (no downing) but if there is an unreachability problem
at the same time as the rolling update is in progress there could be an SBR decision. To avoid that
the total number of members limit is not exceeded during the rolling update it's recommended to
leave and fully remove one node before adding a new one, when using `static-quorum`.

If the cluster is split into 3 (or more) parts each part that is smaller than then configured `quorum-size`
will down itself and possibly shutdown the whole cluster.

If more nodes than the configured `quorum-size` crash at the same time the other running nodes
will down themselves because they think that they are not in the majority, and thereby the whole
cluster is terminated.

The decision can be based on nodes with a configured `role` instead of all nodes in the cluster.
This can be useful when some types of nodes are more valuable than others. You might, for example,
have some nodes responsible for persistent data and some nodes with stateless worker services.
Then it probably more important to keep as many persistent data nodes as possible even though
it means shutting down more worker nodes.

There is another use of the `role` as well. By defining a `role` for a few (e.g. 7) stable
nodes in the cluster and using that in the configuration of `static-quorum` you will be able
to dynamically add and remove other nodes without this role and still have good decisions of what
nodes to keep running and what nodes to shut down in the case of network partitions. The advantage
of this approach compared to `keep-majority` (described below) is that you *do not* risk splitting
the cluster into two separate clusters, i.e. *a split brain**. You must still obey the rule of not
starting too many nodes with this `role` as described above. It also suffers the risk of shutting
down all nodes if there is a failure when there are not enough nodes with this `role` remaining 
in the cluster, as described above.

Configuration:

```
akka.cluster.split-brain-resolver.active-strategy=static-quorum
```

@@snip [reference.conf](/akka-cluster/src/main/resources/reference.conf) { #static-quorum }

### Keep Oldest

The strategy named `keep-oldest` will down the part that does not contain the oldest
member. The oldest member is interesting because the active Cluster Singleton instance
is running on the oldest member.

There is one exception to this rule if `down-if-alone` is configured to `on`.
Then, if the oldest node has partitioned from all other nodes the oldest will down itself
and keep all other nodes running. The strategy will not down the single oldest node when
it is the only remaining node in the cluster.

Note that if the oldest node crashes the others will remove it from the cluster
when `down-if-alone` is `on`, otherwise they will down themselves if the
oldest node crashes, i.e. shut down the whole cluster together with the oldest node.

This strategy is good to use if you use Cluster Singleton and do not want to shut down the node
where the singleton instance runs. If the oldest node crashes a new singleton instance will be
started on the next oldest node. The drawback is that the strategy may keep only a few nodes
in a large cluster. For example, if one part with the oldest consists of 2 nodes and the
other part consists of 98 nodes then it will keep 2 nodes and shut down 98 nodes.

This strategy also handles the edge case that may occur when there are membership changes at the same
time as the network partition occurs. For example, the status of the oldest member is changed to `Exiting`
on one side but that information is not disseminated to the other side before the connection is broken.
It will detect this situation and make the safe decision to down all nodes on the side that sees the oldest as
`Leaving`. Note that this has the drawback that if the oldest was `Leaving` and not changed to `Exiting` then
each part will shut down itself, terminating the whole cluster.

The decision can be based on nodes with a configured `role` instead of all nodes in the cluster,
i.e. using the oldest member (singleton) within the nodes with that role.

Configuration:

```
akka.cluster.split-brain-resolver.active-strategy=keep-oldest
```

@@snip [reference.conf](/akka-cluster/src/main/resources/reference.conf) { #keep-oldest }

### Down All

The strategy named `down-all` will down all nodes.

This strategy can be a safe alternative if the network environment is highly unstable with unreachability observations
that can't be fully trusted, and including frequent occurrences of @ref:[indirectly connected nodes](#indirectly-connected-nodes).
Due to the instability there is an increased risk of different information on different sides of partitions and
therefore the other strategies may result in conflicting decisions. In such environments it can be better to shutdown
all nodes and start up a new fresh cluster.

Shutting down all nodes means that the system will be completely unavailable until nodes have been restarted and
formed a new cluster. This strategy is not recommended for large clusters (> 10 nodes) because any minor problem
will shutdown all nodes, and that is more likely to happen in larger clusters since there are more nodes that
may fail.

See also @ref[Down all when unstable](#down-all-when-unstable) and @ref:[indirectly connected nodes](#indirectly-connected-nodes).

### Lease

The strategy named `lease-majority` is using a distributed lease (lock) to decide what nodes that are allowed to
survive. Only one SBR instance can acquire the lease and make the decision to remain up. The other side will
not be able to aquire the lease and will therefore down itself.

Best effort is to keep the side that has most nodes, i.e. the majority side. This is achieved by adding a delay
before trying to acquire the lease on the minority side.

There is currently one supported implementation of the lease which is backed by a
[Custom Resource Definition (CRD)](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)
in Kubernetes. It is described in the @extref:[Kubernetes Lease](akka-management:kubernetes-lease.html)
documentation.

This strategy is very safe since coordination is added by an external arbiter. The trade-off compared to other
strategies is that it requires additional infrastructure for implementing the lease and it reduces the availability
of a decision to that of the system backing the lease store.

Similar to other strategies it is important that decisions are not deferred for too long because the nodes that couldn't
acquire the lease must decide to down themselves, see @ref[Down all when unstable](#down-all-when-unstable).

In some cases the lease will be unavailable when needed for a decision from all SBR instances, e.g. because it is
on another side of a network partition, and then all nodes will be downed.

Configuration:

```
akka {
  cluster {
    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver {
      active-strategy = "lease-majority"
      lease-majority {
        lease-implementation = "akka.coordination.lease.kubernetes"
      }
    }
  }
}
```

@@snip [reference.conf](/akka-cluster/src/main/resources/reference.conf) { #lease-majority }

See also configuration and additional dependency in @extref:[Kubernetes Lease](akka-management:kubernetes-lease.html)

## Indirectly connected nodes

In a malfunctioning network there can be situations where nodes are observed as unreachable via some network
links, but they are still indirectly connected via other nodes, i.e. it's not a clean network partition (or node crash).

When this situation is detected the Split Brain Resolvers will keep fully connected nodes and down all the indirectly
connected nodes.

If there is a combination of indirectly connected nodes and a clean network partition it will combine the
above decision with the ordinary decision, e.g. keep majority, after excluding suspicious failure detection
observations.

### Down all when indirectly connected

Addition precaution can be enabled for the uncertainty that may follow from indirectly connected nodes
by enabling downing of all nodes if indirectly connected nodes are detected:

```
akka.cluster.split-brain-resolver {
  down-all-when-indirectly-connected = on
}
```

## Down all when unstable

When reachability observations by the failure detector are changed the SBR decisions
are deferred until there are no changes within the `stable-after` duration.
If this continues for too long it might be an indication of an unstable system/network
and it could result in delayed or conflicting decisions on separate sides of a network
partition.

As a precaution for that scenario all nodes are downed if no decision is made within
`stable-after + down-all-when-unstable` from the first unreachability event.
The measurement is reset if all unreachable have been healed, downed or removed.

This is enabled by default for all strategies and by default the duration is derived to
be 3/4 of `stable-after`.

The below property can be defined as a duration of for how long the changes are acceptable to
continue after the `stable-after` or it can be set to `off` to disable this feature.


```
akka.cluster.split-brain-resolver {
  down-all-when-unstable = 15s
  stable-after = 20s
}
```

@@@ warning

It is recommended to keep `down-all-when-unstable` enabled and not set it to a longer duration than `stable-after`
(`down-removal-margin`) because that can result in delayed decisions on the side that should have been downed, e.g.
in the case of a clean network partition followed by continued instability on the side that should be downed.
That could result in that members are removed from one side but are still running on the other side.

@@@

## Cluster Singleton and Cluster Sharding

The purpose of Cluster Singleton and Cluster Sharding is to run at most one instance
of a given actor at any point in time. When such an instance is shut down a new instance
is supposed to be started elsewhere in the cluster. It is important that the new instance is
not started before the old instance has been stopped. This is especially important when the
singleton or the sharded instance is persistent, since there must only be one active
writer of the journaled events of a persistent actor instance.

Since the strategies on different sides of a network partition cannot communicate with each other
and they may take the decision at slightly different points in time there must be a time based
margin that makes sure that the new instance is not started before the old has been stopped.

You would like to configure this to a short duration to have quick failover, but that will increase the
risk of having multiple singleton/sharded instances running at the same time and it may take a different
amount of time to act on the decision (dissemination of the down/removal). The duration is by default
the same as the `stable-after` property (see @ref:[Stable after](#stable-after) above). It is recommended to
leave this value as is, but it can also be separately overriden with the `akka.cluster.down-removal-margin` property.

Another concern for setting this `stable-after`/`akka.cluster.down-removal-margin` is dealing with JVM pauses e.g.
garbage collection. When a node is unresponsive it is not known if it is due to a pause, overload, a crash or a 
network partition. If it is pause that lasts longer than `stable-after` * 2 it gives time for SBR to down the node
and for singletons and shards to be started on other nodes. When the node un-pauses there will be a short time before
it sees its self as down where singletons and sharded actors are still running. It is therefore important to understand
the max pause time your application is likely to incur and make sure it is smaller than `stable-margin`.

If you choose to set a separate value for `down-removal-margin`, the recommended minimum duration for different cluster sizes are:

|cluster size | down-removal-margin|
|-------------|--------------------|
|5    | 7 s |
|10   | 10 s|
|20   | 13 s|
|50   | 17 s|
|100  | 20 s|
|1000 | 30 s|

### Expected Failover Time

As you have seen, there are several configured timeouts that add to the total failover latency.
With default configuration those are:

 * failure detection 5 seconds
 * stable-after 20 seconds
 * down-removal-margin (by default the same as stable-after) 20 seconds

In total, you can expect the failover time of a singleton or sharded instance to be around 45 seconds
with default configuration. The default configuration is sized for a cluster of 100 nodes. If you have
around 10 nodes you can reduce the `stable-after` to around 10 seconds, 
resulting in an expected failover time of around 25 seconds.   
