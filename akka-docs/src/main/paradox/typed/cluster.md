---
project.description: Build distributed applications that scale across the network with Akka Cluster, a fault-tolerant decentralized peer-to-peer based cluster node membership service with no single point of failure.
---
# Cluster Usage
  
This document describes how to use Akka Cluster and the Cluster APIs. 
The [Stateful or Stateless Applications: To Akka Cluster or not](https://www.youtube.com/watch?v=CiVsKjZV-Ys) video is a good starting point to understand the motivation to use Akka Cluster.

For specific documentation topics see: 

* @ref:[When and where to use Akka Cluster](choosing-cluster.md)
* @ref:[Cluster Specification](cluster-concepts.md)
* @ref:[Cluster Membership Service](cluster-membership.md)
* @ref:[Higher level Cluster tools](#higher-level-cluster-tools)
* @ref:[Rolling Updates](../additional/rolling-updates.md)
* @ref:[Operating, Managing, Observability](../additional/operations.md)

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Cluster](../cluster-usage.md).

You have to enable @ref:[serialization](../serialization.md)  to send messages between ActorSystems (nodes) in the Cluster.
@ref:[Serialization with Jackson](../serialization-jackson.md) is a good choice in many cases, and our
recommendation if you don't have other preferences or constraints.

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Cluster add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-typed_$scala.binary.version$
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster-typed" }

## Cluster API Extension

The Cluster extension gives you access to management tasks such as @ref:[Joining, Leaving and Downing](cluster-membership.md#user-actions)
and subscription of cluster membership events such as @ref:[MemberUp, MemberRemoved and UnreachableMember](cluster-membership.md#membership-lifecycle),
which are exposed as event APIs.  

It does this through these references on the @apidoc[typed.Cluster$] extension:

* `manager`: An @scala[@apidoc[typed.ActorRef]\[@apidoc[akka.cluster.typed.ClusterCommand](typed.ClusterCommand)\]]@java[@apidoc[typed.ActorRef]<@apidoc[akka.cluster.typed.ClusterCommand](typed.ClusterCommand)>] where a `ClusterCommand` is a command such as: @apidoc[Join], @apidoc[Leave] and @apidoc[Down]
* `subscriptions`: An @scala[@apidoc[typed.ActorRef]\[@apidoc[akka.cluster.typed.ClusterStateSubscription](typed.ClusterStateSubscription)\]]@java[@apidoc[typed.ActorRef]<@apidoc[akka.cluster.typed.ClusterStateSubscription](typed.ClusterStateSubscription)>] where a `ClusterStateSubscription` is one of @apidoc[GetCurrentState] or @apidoc[Subscribe] and @apidoc[Unsubscribe] to cluster events like @apidoc[MemberRemoved](ClusterEvent.MemberRemoved)
* `state`: The current @apidoc[CurrentClusterState](ClusterEvent.CurrentClusterState)

All of the examples below assume the following imports:

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #cluster-imports }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #cluster-imports }

<a id="basic-cluster-configuration"></a>
The minimum configuration required is to set a host/port for remoting and the `akka.actor.provider = "cluster"`.

@@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #config-seeds }

Accessing the @apidoc[typed.Cluster$] extension on each node:

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #cluster-create }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #cluster-create }

@@@ note
  
  The name of the cluster's @apidoc[typed.ActorSystem] must be the same for all members, which is passed in when you start the `ActorSystem`.

@@@

### Joining and Leaving a Cluster 

If not using configuration to specify @ref:[seed nodes to join](#joining), joining the cluster can be done programmatically via the @scala[@scaladoc[manager](akka.cluster.typed.Cluster#manager:akka.actor.typed.ActorRef[akka.cluster.typed.ClusterCommand])]@java[@javadoc[manager()](akka.cluster.typed.Cluster#manager())].

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #cluster-join }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #cluster-join }

@ref:[Leaving](#leaving) the cluster and @ref:[downing](#downing) a node are similar:

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #cluster-leave }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #cluster-leave }

### Cluster Subscriptions

Cluster @scala[@scaladoc[subscriptions](akka.cluster.typed.Cluster#subscriptions:akka.actor.typed.ActorRef[akka.cluster.typed.ClusterStateSubscription])]@java[@javadoc[subscriptions()](akka.cluster.typed.Cluster#subscriptions())] can be used to receive messages when cluster state changes. For example, registering
for all @apidoc[MemberEvent](ClusterEvent.MemberEvent)'s, then using the `manager` to have a node leave the cluster will result in events
for the node going through the @ref:[Membership Lifecycle](cluster-membership.md#membership-lifecycle).

This example subscribes to a @scala[`subscriber: ActorRef[MemberEvent]`]@java[`ActorRef<MemberEvent> subscriber`]:

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #cluster-subscribe }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #cluster-subscribe }

Then asking a node to leave:

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #cluster-leave-example }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #cluster-leave-example }


### Cluster State

Instead of subscribing to cluster events it can sometimes be convenient to only get the full membership state with
@scala[@scaladoc[Cluster(system).state](akka.cluster.typed.Cluster#state:akka.cluster.ClusterEvent.CurrentClusterState)]@java[@javadoc[Cluster.get(system).state()](akka.cluster.typed.Cluster#state())]. Note that this state is not necessarily in sync with the events published to a
cluster subscription.

See @ref:[Cluster Membership](cluster-membership.md#member-events) more information on member events specifically.
There are more types of change events, consult the API documentation
of classes that extends @apidoc[akka.cluster.ClusterEvent.ClusterDomainEvent](ClusterEvent.ClusterDomainEvent) for details about the events.
 
## Cluster Membership API

### Joining
 
The seed nodes are initial contact points for joining a cluster, which can be done in different ways:

* @ref:[automatically with Cluster Bootstrap](#joining-automatically-to-seed-nodes-with-cluster-bootstrap)
* @ref:[with configuration of seed-nodes](#joining-configured-seed-nodes)
* @ref:[programatically](#joining-programmatically-to-seed-nodes)
 
After the joining process the seed nodes are not special and they participate in the cluster in exactly the same
way as other nodes.

#### Joining automatically to seed nodes with Cluster Bootstrap

Automatic discovery of nodes for the joining process is available
using the Akka Management project's module, 
@ref:[Cluster Bootstrap](../additional/operations.md#cluster-bootstrap).
Please refer to its documentation for more details.

#### Joining configured seed nodes

When a new node is started it sends a message to all seed nodes and then sends a join command to the one that
answers first. If none of the seed nodes replies (might not be started yet)
it retries this procedure until success or shutdown.

You can define the seed nodes in the @ref:[configuration](#configuration) file (application.conf):

```
akka.cluster.seed-nodes = [
  "akka://ClusterSystem@host1:2552",
  "akka://ClusterSystem@host2:2552"]
```

This can also be defined as Java system properties when starting the JVM using the following syntax:

```
-Dakka.cluster.seed-nodes.0=akka://ClusterSystem@host1:2552
-Dakka.cluster.seed-nodes.1=akka://ClusterSystem@host2:2552
```


When a new node is started it sends a message to all configured `seed-nodes` and then sends a join command to the
one that answers first. If none of the seed nodes replies (might not be started yet) it retries this procedure
until successful or shutdown.

The seed nodes can be started in any order. It is not necessary to have all
seed nodes running, but the node configured as the **first element** in the `seed-nodes`
list must be started when initially starting a cluster. If it is not, the
other seed-nodes will not become initialized, and no other node can join the cluster.
The reason for the special first seed node is to avoid forming separated islands when
starting from an empty cluster.
It is quickest to start all configured seed nodes at the same time (order doesn't matter),
otherwise it can take up to the configured `seed-node-timeout` until the nodes
can join.

As soon as more than two seed nodes have been started, it is no problem to shut down the first
seed node. If the first seed node is restarted, it will first try to join the other
seed nodes in the existing cluster. Note that if you stop all seed nodes at the same time
and restart them with the same `seed-nodes` configuration they will join themselves and
form a new cluster, instead of joining remaining nodes of the existing cluster. That is
likely not desired and can be avoided by listing several nodes as seed nodes for redundancy,
and don't stop all of them at the same time.

If you are going to start the nodes on different machines you need to specify the
ip-addresses or host names of the machines in `application.conf` instead of `127.0.0.1`

#### Joining programmatically to seed nodes

Joining programmatically is useful when **dynamically discovering** other nodes
at startup through an external tool or API.

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #join-seed-nodes }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #join-seed-nodes }

The seed node address list has the same semantics as the configured `seed-nodes`, and the the underlying
implementation of the process is the same, see @ref:[Joining configured seed nodes](#joining-configured-seed-nodes).

When joining to seed nodes you should not include the node itself, except for the node that is supposed to be the
first seed node bootstrapping the cluster. The desired initial seed node address should be placed first in the parameter to the programmatic
join.
           
#### Tuning joins

Unsuccessful attempts to contact seed nodes are automatically retried after the time period defined in
configuration property `seed-node-timeout`. Unsuccessful attempts to join a specific seed node are
automatically retried after the configured `retry-unsuccessful-join-after`. Retrying means that it
tries to contact all seed nodes, then joins the node that answers first. The first node in the list
of seed nodes will join itself if it cannot contact any of the other seed nodes within the
configured `seed-node-timeout`.

The joining of given seed nodes will, by default, be retried indefinitely until
a successful join. That process can be aborted if unsuccessful by configuring a
timeout. When aborted it will run @ref:[Coordinated Shutdown](../coordinated-shutdown.md),
which will terminate the ActorSystem by default. CoordinatedShutdown can also be configured to exit
the JVM. If the `seed-nodes` are assembled dynamically, it is useful to define this timeout,
and a restart with new seed-nodes should be tried after unsuccessful attempts.

```
akka.cluster.shutdown-after-unsuccessful-join-seed-nodes = 20s
akka.coordinated-shutdown.exit-jvm = on
```

If you don't configure seed nodes or use one of the join seed node functions, you need to join the cluster manually
by using @ref:[JMX](../additional/operations.md#jmx) or @ref:[HTTP](../additional/operations.md#http).

You can join to any node in the cluster. It does not have to be configured as a seed node.
Note that you can only join to an existing cluster member, which for bootstrapping means a
node must join itself and subsequent nodes could join them to make up a cluster.

An actor system can only join a cluster once, additional attempts will be ignored.
Once an actor system has successfully joined a cluster, it would have to be restarted to join the same cluster again. 
It can use the same host name and port after the restart. When it come up as a new incarnation of an existing member in the cluster 
and attempts to join, the existing member will be removed and its new incarnation allowed to join.

### Leaving

There are a few ways to remove a member from the cluster.

1. The recommended way to leave a cluster is a graceful exit, informing the cluster that a node shall leave.
  This is performed by @ref:[Coordinated Shutdown](../coordinated-shutdown.md) when the @apidoc[typed.ActorSystem]
  is terminated and also when a SIGTERM is sent from the environment to stop the JVM process.
1. Graceful exit can also be performed using @ref:[HTTP](../additional/operations.md#http) or @ref:[JMX](../additional/operations.md#jmx). 
1. When a graceful exit is not possible, for example in case of abrupt termination of the the JVM process, the node
  will be detected as unreachable by other nodes and removed after @ref:[Downing](#downing).

Graceful leaving offers faster hand off to peer nodes during node shutdown than abrupt termination and downing.

The @ref:[Coordinated Shutdown](../coordinated-shutdown.md) will also run when the cluster node sees itself as
`Exiting`, i.e. leaving from another node will trigger the shutdown process on the leaving node.
Tasks for graceful leaving of cluster, including graceful shutdown of Cluster Singletons and
Cluster Sharding, are added automatically when Akka Cluster is used. For example, running the shutdown
process will also trigger the graceful leaving if not already in progress.

Normally this is handled automatically, but in case of network failures during this process it may still
be necessary to set the node’s status to `Down` in order to complete the removal, see @ref:[Downing](#downing).

### Downing

In many cases a member can gracefully exit from the cluster, as described in @ref:[Leaving](#leaving), but
there are scenarios when an explicit downing decision is needed before it can be removed. For example in case
of abrupt termination of the the JVM process, system overload that doesn't recover, or network partitions
that don't heal. In such cases, the node(s) will be detected as unreachable by other nodes, but they must also
be marked as `Down` before they are removed.

When a member is considered by the failure detector to be `unreachable` the
leader is not allowed to perform its duties, such as changing status of
new joining members to 'Up'. The node must first become `reachable` again, or the
status of the unreachable member must be changed to `Down`. Changing status to `Down`
can be performed automatically or manually.

We recommend that you enable the @ref:[Split Brain Resolver](../split-brain-resolver.md) that is part of the
Akka Cluster module. You enable it with configuration:

```
akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
```

You should also consider the different available @ref:[downing strategies](../split-brain-resolver.md#strategies).

If a downing provider is not configured downing must be performed manually using 
@ref:[HTTP](../additional/operations.md#http) or @ref:[JMX](../additional/operations.md#jmx).

Note that @ref:[Cluster Singleton](cluster-singleton.md) or @ref:[Cluster Sharding entities](cluster-sharding.md) that
are running on a crashed (unreachable) node will not be started on another node until the previous node has
been removed from the Cluster. Removal of crashed (unreachable) nodes is performed after a downing decision.

Downing can also be performed programmatically with @scala[`Cluster(system).manager ! Down(address)`]@java[`Cluster.get(system).manager().tell(Down(address))`],
but that is mostly useful from tests and when implementing a @apidoc[DowningProvider].

If a crashed node is restarted and joining the cluster again with the same hostname and port, the previous incarnation
of that member will first be downed and removed. The new join attempt with same hostname and port is used as evidence
that the previous is no longer alive.

If a node is still running and sees its self as `Down` it will shutdown. @ref:[Coordinated Shutdown](../coordinated-shutdown.md) will automatically
run if `run-coordinated-shutdown-when-down` is set to `on` (the default) however the node will not try
and leave the cluster gracefully.

## Node Roles

Not all nodes of a cluster need to perform the same function. For example, there might be one sub-set which runs the web front-end,
one which runs the data access layer and one for the number-crunching. Choosing which actors to start on each node,
for example cluster-aware routers, can take node roles into account to achieve this distribution of responsibilities.

The node roles are defined in the configuration property named `akka.cluster.roles`
and typically defined in the start script as a system property or environment variable.

The roles are part of the membership information in @apidoc[MemberEvent](ClusterEvent.MemberEvent) that you can subscribe to. The roles
of the own node are available from the @scala[@scaladoc[selfMember](akka.cluster.typed.Cluster#selfMember:akka.cluster.Member)]@java[@javadoc[selfMember()](akka.cluster.typed.Cluster#selfMember())] and that can be used for conditionally starting certain
actors:

Scala
:  @@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #hasRole }

Java
:  @@snip [BasicClusterExampleTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/BasicClusterExampleTest.java) { #hasRole }

## Failure Detector

The nodes in the cluster monitor each other by sending heartbeats to detect if a node is
unreachable from the rest of the cluster. Please see:

* @ref:[Failure Detector specification](cluster-concepts.md#failure-detector)
* @ref:[Phi Accrual Failure Detector](failure-detector.md) implementation
* @ref:[Using the Failure Detector](#using-the-failure-detector) 
 
### Using the Failure Detector
 
Cluster uses the @apidoc[akka.remote.PhiAccrualFailureDetector](PhiAccrualFailureDetector) failure detector by default, or you can provide your by
implementing the @apidoc[akka.remote.FailureDetector](FailureDetector) and configuring it:

```
akka.cluster.implementation-class = "com.example.CustomFailureDetector"
```

In the @ref:[Cluster Configuration](#configuration) you may want to adjust these
depending on you environment:

* When a *phi* value is considered to be a failure `akka.cluster.failure-detector.threshold`
* Margin of error for sudden abnormalities `akka.cluster.failure-detector.acceptable-heartbeat-pause`  

## How to test

Akka comes with and uses several types of testing strategies:

* @ref:[Testing](testing.md)
* @ref:[Multi Node Testing](../multi-node-testing.md)
* @ref:[Multi JVM Testing](../multi-jvm-testing.md)

## Configuration

There are several configuration properties for the cluster. Refer to the 
@ref:[reference configuration](../general/configuration-reference.md#config-akka-cluster) for full
configuration descriptions, default values and options.

### How To Startup when a Cluster size is reached

A common use case is to start actors after the cluster has been initialized,
members have joined, and the cluster has reached a certain size.

With a configuration option you can define the required number of members
before the leader changes member status of 'Joining' members to 'Up'.:

```
akka.cluster.min-nr-of-members = 3
```

In a similar way you can define the required number of members of a certain role
before the leader changes member status of 'Joining' members to 'Up'.:

```
akka.cluster.role {
  frontend.min-nr-of-members = 1
  backend.min-nr-of-members = 2
}
```

### Cluster Info Logging

You can silence the logging of cluster events at info level with configuration property:

```
akka.cluster.log-info = off
```

You can enable verbose logging of cluster events at info level, e.g. for temporary troubleshooting, with configuration property:

```
akka.cluster.log-info-verbose = on
```

### Cluster Dispatcher

The Cluster extension is implemented with actors. To protect them against
disturbance from user actors they are by default run on the internal dispatcher configured
under `akka.actor.internal-dispatcher`. The cluster actors can potentially be isolated even
further, onto their own dispatcher using the setting `akka.cluster.use-dispatcher`
or made run on the same dispatcher to keep the number of threads down.

### Configuration Compatibility Check

Creating a cluster is about deploying two or more nodes and having them behave as if they were a single application. Therefore it's extremely important that all nodes in a cluster are configured with compatible settings. 

The Configuration Compatibility Check feature ensures that all nodes in a cluster have a compatible configuration. Whenever a new node is joining an existing cluster, a subset of its configuration settings (only those that are required to be checked) is sent to the nodes in the cluster for verification. Once the configuration is checked on the cluster side, the cluster sends back its own set of required configuration settings. The joining node will then verify if it's compliant with the cluster configuration. The joining node will only proceed if all checks pass, on both sides.   

New custom checkers can be added by extending @apidoc[akka.cluster.JoinConfigCompatChecker](JoinConfigCompatChecker) and including them in the configuration. Each checker must be associated with a unique key:

```
akka.cluster.configuration-compatibility-check.checkers {
  my-custom-config = "com.company.MyCustomJoinConfigCompatChecker"
}
``` 

@@@ note

Configuration Compatibility Check is enabled by default, but can be disabled by setting `akka.cluster.configuration-compatibility-check.enforce-on-join = off`. This is specially useful when performing rolling updates. Obviously this should only be done if a complete cluster shutdown isn't an option. A cluster with nodes with different configuration settings may lead to data loss or data corruption. 

This setting should only be disabled on the joining nodes. The checks are always performed on both sides, and warnings are logged. In case of incompatibilities, it is the responsibility of the joining node to decide if the process should be interrupted or not.  

If you are performing a rolling update on cluster using Akka 2.5.9 or prior (thus, not supporting this feature), the checks will not be performed because the running cluster has no means to verify the configuration sent by the joining node, nor to send back its own configuration.  

@@@ 

## Higher level Cluster tools

@@include[cluster.md](../includes/cluster.md) { #cluster-singleton } 
See @ref:[Cluster Singleton](cluster-singleton.md).
 
@@include[cluster.md](../includes/cluster.md) { #cluster-sharding }  
See @ref:[Cluster Sharding](cluster-sharding.md).
 
@@include[cluster.md](../includes/cluster.md) { #cluster-ddata } 
See @ref:[Distributed Data](distributed-data.md).

@@include[cluster.md](../includes/cluster.md) { #cluster-pubsub }
See @ref:[Distributed Publish Subscribe](distributed-pub-sub.md).

@@include[cluster.md](../includes/cluster.md) { #cluster-router }
See @ref:[Group Routers](routers.md#group-router). 

@@include[cluster.md](../includes/cluster.md) { #reliable-delivery }
See @ref:[Reliable Delivery](reliable-delivery.md)

## Example project

@java[[Cluster example project](https://github.com/akka/akka-core/tree/main/samples/akka-sample-cluster-java#readme) [(sources zip)](../attachments/akka-samples-cluster-java.zip)]
@scala[[Cluster example project](https://github.com/akka/akka-core/tree/main/samples/akka-sample-cluster-scala#readme) [(sources zip)](../attachments/akka-samples-cluster-scala.zip)]
is an example project that can be downloaded, and with instructions of how to run.

This project contains samples illustrating different Cluster features, such as
subscribing to cluster membership events, and sending messages to actors running on nodes in the cluster
with Cluster aware routers.
