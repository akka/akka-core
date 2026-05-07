# Classic Cluster Usage

@@include[includes.md](includes.md) { #actor-api } 
For the full documentation of this feature and for new projects see @ref:[Cluster](typed/cluster.md).
For specific documentation topics see:  

* @ref:[Cluster Specification](typed/cluster-concepts.md)
* @ref:[Cluster Membership Service](typed/cluster-membership.md)
* @ref:[When and where to use Akka Cluster](typed/choosing-cluster.md)
* @ref:[Higher level Cluster tools](#higher-level-cluster-tools)
* @ref:[Rolling Updates](additional/rolling-updates.md)
* @ref:[Operating, Managing, Observability](additional/operations.md)

@@@ note

You have to enable @ref:[serialization](serialization.md) to send messages between ActorSystems in the Cluster.
@ref:[Serialization with Jackson](serialization-jackson.md) is a good choice in many cases, and our
recommendation if you don't have other preferences or constraints.

@@@

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Cluster add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-cluster_$scala.binary.version$"
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster" }

## When and where to use Akka Cluster
 
See @ref:[Choosing Akka Cluster](typed/choosing-cluster.md#when-and-where-to-use-akka-cluster) in the documentation of the new APIs.

## Cluster API Extension

The following configuration enables the @apidoc[cluster.Cluster] extension to be used.
It joins the cluster and an actor subscribes to cluster membership events and logs them.

An actor that uses the cluster extension may look like this:

Scala
:  @@snip [SimpleClusterListener.scala](/akka-docs/src/test/scala/docs/cluster/SimpleClusterListener.scala) { type=scala }

Java
:  @@snip [SimpleClusterListener.java](/akka-docs/src/test/java/jdocs/cluster/SimpleClusterListener.java) { type=java }

And the minimum configuration required is to set a host/port for remoting and the `akka.actor.provider = "cluster"`.

@@snip [BasicClusterExampleSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/BasicClusterExampleSpec.scala) { #config-seeds }

The actor registers itself as subscriber of certain cluster events. It receives events corresponding to the current state
of the cluster when the subscription starts and then it receives events for changes that happen in the cluster.

## Cluster Membership API

This section shows the basic usage of the membership API. For the in-depth descriptions on joining, joining to seed nodes, downing and leaving of any node in the cluster please see the full 
@ref:[Cluster Membership API](typed/cluster.md#cluster-membership-api) documentation.
  
### Joining to Seed Nodes

The seed nodes are initial contact points for joining a cluster, which can be done in different ways:

* @ref:[automatically with Cluster Bootstrap](typed/cluster.md#joining-automatically-to-seed-nodes-with-cluster-bootstrap)
* @ref:[with configuration of seed-nodes](typed/cluster.md#joining-configured-seed-nodes)
* @ref:[programatically](#joining-programmatically-to-seed-nodes)
 
After the joining process the seed nodes are not special and they participate in the cluster in exactly the same
way as other nodes.

#### Joining programmatically to seed nodes

You may also join programmatically, which is attractive when dynamically discovering other nodes
at startup by using some external tool or API.

Scala
:  @@snip [ClusterDocSpec.scala](/akka-docs/src/test/scala/docs/cluster/ClusterDocSpec.scala) { #join-seed-nodes }

Java
:  @@snip [ClusterDocTest.java](/akka-docs/src/test/java/jdocs/cluster/ClusterDocTest.java) { #join-seed-nodes-imports #join-seed-nodes }

For more information see @ref[tuning joins](typed/cluster.md#tuning-joins)

It's also possible to specifically join a single node as illustrated in below example, but @apidoc[joinSeedNodes](cluster.Cluster) {scala="#joinSeedNodes(seedNodes:Seq[akka.actor.Address]):Unit" java="#joinSeedNodes(java.util.List)"} should be
preferred since it has redundancy and retry mechanisms built-in.

Scala
:  @@snip [SimpleClusterListener2.scala](/akka-docs/src/test/scala/docs/cluster/SimpleClusterListener2.scala) { #join }

Java
:  @@snip [SimpleClusterListener2.java](/akka-docs/src/test/java/jdocs/cluster/SimpleClusterListener2.java) { #join }

## Leaving

See @ref:[Leaving](typed/cluster.md#leaving) in the documentation of the new APIs.

## Downing

See @ref:[Downing](typed/cluster.md#downing) in the documentation of the new APIs.

<a id="cluster-subscriber"></a>
## Subscribe to Cluster Events

You can subscribe to change notifications of the cluster membership by using
@scala[@scaladoc[Cluster(system).subscribe](akka.cluster.Cluster#subscribe(subscriber:akka.actor.ActorRef,to:Class[_]*):Unit)]@java[@javadoc[Cluster.get(system).subscribe](akka.cluster.Cluster#subscribe(akka.actor.ActorRef,akka.cluster.ClusterEvent.SubscriptionInitialStateMode,java.lang.Class...))].

Scala
:  @@snip [SimpleClusterListener2.scala](/akka-docs/src/test/scala/docs/cluster/SimpleClusterListener2.scala) { #subscribe }

Java
:  @@snip [SimpleClusterListener2.java](/akka-docs/src/test/java/jdocs/cluster/SimpleClusterListener2.java) { #subscribe }

A snapshot of the full state, @apidoc[CurrentClusterState](ClusterEvent.CurrentClusterState), is sent to the subscriber
as the first message, followed by events for incremental updates.

Note that you may receive an empty `CurrentClusterState`, containing no members,
followed by @apidoc[MemberUp](ClusterEvent.MemberUp) events from other nodes which already joined,
if you start the subscription before the initial join procedure has completed.
This may for example happen when you start the subscription immediately after @apidoc[cluster.join()](cluster.Cluster) {scala="#join(address:akka.actor.Address):Unit" java="#join(akka.actor.Address)"} like below.
This is expected behavior. When the node has been accepted in the cluster you will
receive `MemberUp` for that node, and other nodes.

Scala
:  @@snip [SimpleClusterListener2.scala](/akka-docs/src/test/scala/docs/cluster/SimpleClusterListener2.scala) { #join #subscribe }

Java
:  @@snip [SimpleClusterListener2.java](/akka-docs/src/test/java/jdocs/cluster/SimpleClusterListener2.java) { #join #subscribe }

To avoid receiving an empty `CurrentClusterState` at the beginning, you can use it like shown in the following example,
to defer subscription until the `MemberUp` event for the own node is received:

Scala
:  @@snip [SimpleClusterListener2.scala](/akka-docs/src/test/scala/docs/cluster/SimpleClusterListener2.scala) { #join #register-on-memberup }

Java
:  @@snip [SimpleClusterListener2.java](/akka-docs/src/test/java/jdocs/cluster/SimpleClusterListener2.java) { #join #register-on-memberup }


If you find it inconvenient to handle the `CurrentClusterState` you can use
@scala[@scaladoc[ClusterEvent.InitialStateAsEvents](akka.cluster.ClusterEvent$$InitialStateAsEvents$)] @java[@javadoc[ClusterEvent.initialStateAsEvents()](akka.cluster.ClusterEvent#initialStateAsEvents())] as parameter to @apidoc[subscribe](akka.cluster.Cluster) {scala="#subscribe(subscriber:akka.actor.ActorRef,to:Class[_]*):Unit" java="#subscribe(akka.actor.ActorRef,akka.cluster.ClusterEvent.SubscriptionInitialStateMode,java.lang.Class...)"}.
That means that instead of receiving `CurrentClusterState` as the first message you will receive
the events corresponding to the current state to mimic what you would have seen if you were
listening to the events when they occurred in the past. Note that those initial events only correspond
to the current state and it is not the full history of all changes that actually has occurred in the cluster.

Scala
:  @@snip [SimpleClusterListener.scala](/akka-docs/src/test/scala/docs/cluster/SimpleClusterListener.scala) { #subscribe }

Java
:  @@snip [SimpleClusterListener.java](/akka-docs/src/test/java/jdocs/cluster/SimpleClusterListener.java) { #subscribe }

### Worker Dial-in Example

Let's take a look at an example that illustrates how workers, here named *backend*,
can detect and register to new master nodes, here named *frontend*.

The example application provides a service to transform text. When some text
is sent to one of the frontend services, it will be delegated to one of the
backend workers, which performs the transformation job, and sends the result back to
the original client. New backend nodes, as well as new frontend nodes, can be
added or removed to the cluster dynamically.

Messages:

Scala
:  @@snip [TransformationMessages.scala](/akka-docs/src/test/scala/docs/cluster/TransformationMessages.scala) { #messages }

Java
:  @@snip [TransformationMessages.java](/akka-docs/src/test/java/jdocs/cluster/TransformationMessages.java) { #messages }

The backend worker that performs the transformation job:

Scala
:  @@snip [TransformationBackend.scala](/akka-docs/src/test/scala/docs/cluster/TransformationBackend.scala) { #backend }

Java
:  @@snip [TransformationBackend.java](/akka-docs/src/test/java/jdocs/cluster/TransformationBackend.java) { #backend }

Note that the `TransformationBackend` actor subscribes to cluster events to detect new,
potential, frontend nodes, and send them a registration message so that they know
that they can use the backend worker.

The frontend that receives user jobs and delegates to one of the registered backend workers:

Scala
:  @@snip [TransformationFrontend.scala](/akka-docs/src/test/scala/docs/cluster/TransformationFrontend.scala) { #frontend }

Java
:  @@snip [TransformationFrontend.java](/akka-docs/src/test/java/jdocs/cluster/TransformationFrontend.java) { #frontend }

Note that the `TransformationFrontend` actor watch the registered backend
to be able to remove it from its list of available backend workers.
Death watch uses the cluster failure detector for nodes in the cluster, i.e. it detects
network failures and JVM crashes, in addition to graceful termination of watched
actor. Death watch generates the @apidoc[actor.Terminated] message to the watching actor when the
unreachable cluster node has been downed and removed.

## Node Roles

See @ref:[Cluster Node Roles](typed/cluster.md#node-roles) in the documentation of the new APIs.

<a id="min-members"></a>
## How To Startup when Cluster Size Reached

See @ref:[How to startup when a minimum number of members in the cluster is reached](typed/cluster.md#how-to-startup-when-a-cluster-size-is-reached) in the documentation of the new APIs.

## How To Startup when Member is Up

A common use case is to start actors after the cluster has been initialized,
members have joined, and the cluster has reached a certain size.

With a configuration option you can define required number of members
before the leader changes member status of 'Joining' members to 'Up'.:

```
akka.cluster.min-nr-of-members = 3
```

In a similar way you can define required number of members of a certain role
before the leader changes member status of 'Joining' members to 'Up'.:

```
akka.cluster.role {
  frontend.min-nr-of-members = 1
  backend.min-nr-of-members = 2
}
```

You can start actors or trigger any functions using the @apidoc[registerOnMemberUp](cluster.Cluster) {scala="#registerOnMemberUp[T](code:=%3ET):Unit" java="#registerOnMemberUp(java.lang.Runnable)"} callback, which will
be invoked when the current member status is changed to 'Up'. This can additionally be used with 
`akka.cluster.min-nr-of-members` optional configuration to defer an action until the cluster has reached a certain size.

Scala
:  @@snip [FactorialFrontend.scala](/akka-docs/src/test/scala/docs/cluster/FactorialFrontend.scala) { #registerOnUp }

Java
:  @@snip [FactorialFrontendMain.java](/akka-docs/src/test/java/jdocs/cluster/FactorialFrontendMain.java) { #registerOnUp }

## How To Cleanup when Member is Removed

You can do some clean up in a @apidoc[registerOnMemberRemoved](cluster.Cluster) {scala="#registerOnMemberRemoved[T](code:=%3ET):Unit" java="#registerOnMemberRemoved(java.lang.Runnable)"} callback, which will
be invoked when the current member status is changed to 'Removed' or the cluster have been shutdown.

An alternative is to register tasks to the @ref:[Coordinated Shutdown](coordinated-shutdown.md).

@@@ note

Register a OnMemberRemoved callback on a cluster that have been shutdown, the callback will be invoked immediately on
the caller thread, otherwise it will be invoked later when the current member status changed to @scala[@scaladoc[Removed](akka.cluster.MemberStatus$$Removed$)]@java[@javadoc[Removed](akka.cluster.MemberStatus#removed())]. You may
want to install some cleanup handling after the cluster was started up, but the cluster might already be shutting
down when you installing, and depending on the race is not healthy.

@@@

## Higher level Cluster tools

@@include[cluster.md](includes/cluster.md) { #cluster-singleton }
See @ref:[Cluster Singleton](cluster-singleton.md).

@@include[cluster.md](includes/cluster.md) { #cluster-sharding }
See @ref:[Cluster Sharding](cluster-sharding.md).
 
@@include[cluster.md](includes/cluster.md) { #cluster-ddata } 
See @ref:[Distributed Data](distributed-data.md).

@@include[cluster.md](includes/cluster.md) { #cluster-pubsub }
See @ref:[Cluster Distributed Publish Subscribe](distributed-pub-sub.md).

### Cluster Aware Routers

All routers can be made aware of member nodes in the cluster, i.e.
deploying new routees or looking up routees on nodes in the cluster.
When a node becomes unreachable or leaves the cluster the routees of that node are
automatically unregistered from the router. When new nodes join the cluster, additional
routees are added to the router, according to the configuration.
 
See @ref:[Cluster Aware Routers](cluster-routing.md) and @ref:[Routers](routing.md).

### Cluster Metrics

The member nodes of the cluster can collect system health metrics and publish that to other cluster nodes
and to the registered subscribers on the system event bus.

See @ref:[Cluster Metrics](cluster-metrics.md). 
    
## Failure Detector

The nodes in the cluster monitor each other by sending heartbeats to detect if a node is
unreachable from the rest of the cluster. Please see:

* @ref:[Failure Detector specification](typed/cluster-concepts.md#failure-detector)
* @ref:[Phi Accrual Failure Detector](typed/failure-detector.md) implementation
* @ref:[Using the Failure Detector](typed/cluster.md#using-the-failure-detector) 
 
## How to Test

@@@ div { .group-scala }

@ref:[Multi Node Testing](multi-node-testing.md) is useful for testing cluster applications.

Set up your project according to the instructions in @ref:[Multi Node Testing](multi-node-testing.md) and @ref:[Multi JVM Testing](multi-jvm-testing.md), i.e.
add the `sbt-multi-jvm` plugin and the dependency to `akka-multi-node-testkit`.

First, as described in @ref:[Multi Node Testing](multi-node-testing.md), we need some scaffolding to configure the @scaladoc[MultiNodeSpec](akka.remote.testkit.MultiNodeSpec).
Define the participating @ref:[roles](typed/cluster.md#node-roles) and their @ref:[configuration](#configuration) in an object extending @scaladoc[MultiNodeConfig](akka.remote.testkit.MultiNodeConfig):

@@snip [StatsSampleSpec.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsSampleSpec.scala) { #MultiNodeConfig }

Define one concrete test class for each role/node. These will be instantiated on the different nodes (JVMs). They can be
implemented differently, but often they are the same and extend an abstract test class, as illustrated here.

@@snip [StatsSampleSpec.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsSampleSpec.scala) { #concrete-tests }

Note the naming convention of these classes. The name of the classes must end with `MultiJvmNode1`, `MultiJvmNode2`
and so on. It is possible to define another suffix to be used by the `sbt-multi-jvm`, but the default should be
fine in most cases.

Then the abstract `MultiNodeSpec`, which takes the `MultiNodeConfig` as constructor parameter.

@@snip [StatsSampleSpec.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsSampleSpec.scala) { #abstract-test }

Most of this can be extracted to a separate trait to avoid repeating this in all your tests.

Typically you begin your test by starting up the cluster and let the members join, and create some actors.
That can be done like this:

@@snip [StatsSampleSpec.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsSampleSpec.scala) { #startup-cluster }

From the test you interact with the cluster using the `Cluster` extension, e.g. @scaladoc[join](akka.cluster.Cluster#join(address:akka.actor.Address):Unit).

@@snip [StatsSampleSpec.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsSampleSpec.scala) { #join }

Notice how the *testActor* from @ref:[testkit](testing.md) is added as @ref:[subscriber](#cluster-subscriber)
to cluster changes and then waiting for certain events, such as in this case all members becoming 'Up'.

The above code was running for all roles (JVMs). @scaladoc[runOn](akka.remote.testkit.MultiNodeSpec#runOn(nodes:akka.remote.testconductor.RoleName*)(thunk:=%3EUnit):Unit) is a convenient utility to declare that a certain block
of code should only run for a specific role.

@@snip [StatsSampleSpec.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsSampleSpec.scala) { #test-statsService }

Once again we take advantage of the facilities in @ref:[testkit](testing.md) to verify expected behavior.
Here using `testActor` as sender (via @scaladoc[ImplicitSender](akka.testkit.ImplicitSender)) and verifying the reply with @scaladoc[expectMsgType](akka.testkit.TestKit#expectMsgType[T](max:scala.concurrent.duration.FiniteDuration)(implicitt:scala.reflect.ClassTag[T]):T).

In the above code you can see `node(third)`, which is useful facility to get the root actor reference of
the actor system for a specific role. This can also be used to grab the @scaladoc[akka.actor.Address](akka.actor.Address) of that node.

@@snip [StatsSampleSpec.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsSampleSpec.scala) { #addresses }

@@@

@@@ div { .group-java }

Currently testing with the `sbt-multi-jvm` plugin is only documented for Scala.
Go to the corresponding Scala version of this page for details.

@@@

## Management

There are several management tools for the cluster. Please refer to the
@ref:[Cluster Management](additional/operations.md#cluster-management) for more information.
 
<a id="cluster-command-line"></a>
### Command Line

@@@ warning

**Deprecation warning** - The command line script has been deprecated and is scheduled for removal
in the next major version. Use the @ref:[HTTP management](additional/operations.md#http) API with [curl](https://curl.se/)
or similar instead.

@@@

The cluster can be managed with the script `akka-cluster` provided in the Akka GitHub repository @extref[here](github:akka-cluster/jmx-client). Place the script and the `jmxsh-R5.jar` library in the same directory.

Run it without parameters to see instructions about how to use the script:

```
Usage: ./akka-cluster <node-hostname> <jmx-port> <command> ...

Supported commands are:
           join <node-url> - Sends request a JOIN node with the specified URL
          leave <node-url> - Sends a request for node with URL to LEAVE the cluster
           down <node-url> - Sends a request for marking node with URL as DOWN
             member-status - Asks the member node for its current status
                   members - Asks the cluster for addresses of current members
               unreachable - Asks the cluster for addresses of unreachable members
            cluster-status - Asks the cluster for its current status (member ring,
                             unavailable nodes, meta data etc.)
                    leader - Asks the cluster who the current leader is
              is-singleton - Checks if the cluster is a singleton cluster (single
                             node cluster)
              is-available - Checks if the member node is available
Where the <node-url> should be on the format of
  'akka.<protocol>://<actor-system-name>@<hostname>:<port>'

Examples: ./akka-cluster localhost 9999 is-available
          ./akka-cluster localhost 9999 join akka://MySystem@darkstar:2552
          ./akka-cluster localhost 9999 cluster-status
```

To be able to use the script you must enable remote monitoring and management when starting the JVMs of the cluster nodes,
as described in [Monitoring and Management Using JMX Technology](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html).
Make sure you understand the security implications of enabling remote monitoring and management.

<a id="cluster-configuration"></a>

## Configuration

There are several @ref:[configuration](typed/cluster.md#configuration) properties for the cluster,
and the full @ref:[reference configuration](general/configuration-reference.md#config-akka-cluster) for complete information. 
