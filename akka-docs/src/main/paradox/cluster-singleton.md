# Classic Cluster Singleton

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[Cluster Singleton](typed/cluster-singleton.md).

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Cluster Singleton, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-tools_$scala.binary.version$
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster-tools" }

## Introduction

For the full documentation of this feature and for new projects see @ref:[Cluster Singleton - Introduction](typed/cluster-singleton.md#introduction).

The cluster singleton pattern is implemented by `akka.cluster.singleton.ClusterSingletonManager`.
It manages one singleton actor instance among all cluster nodes or a group of nodes tagged with
a specific role. `ClusterSingletonManager` is an actor that is supposed to be started as early as possible
on all nodes, or all nodes with specified role, in the cluster. The actual singleton actor is
started by the `ClusterSingletonManager` on the oldest node by creating a child actor from
supplied `Props`. `ClusterSingletonManager` makes sure that at most one singleton instance
is running at any point in time.

You can access the singleton actor by using the provided `akka.cluster.singleton.ClusterSingletonProxy`,
which will route all messages to the current instance of the singleton. The proxy will keep track of
the oldest node in the cluster and resolve the singleton's `ActorRef` by explicitly sending the
singleton's `actorSelection` the `akka.actor.Identify` message and waiting for it to reply.
This is performed periodically if the singleton doesn't reply within a certain (configurable) time.
Given the implementation, there might be periods of time during which the `ActorRef` is unavailable,
e.g., when a node leaves the cluster. In these cases, the proxy will buffer the messages sent to the
singleton and then deliver them when the singleton is finally available. If the buffer is full
the `ClusterSingletonProxy` will drop old messages when new messages are sent via the proxy.
The size of the buffer is configurable and it can be disabled by using a buffer size of 0.

See @ref:[Cluster Singleton - Potential problems to be aware of](typed/cluster-singleton.md#potential-problems-to-be-aware-of).

## An Example

Assume that we need one single entry point to an external system. An actor that
receives messages from a JMS queue with the strict requirement that only one
JMS consumer must exist to make sure that the messages are processed in order.
That is perhaps not how one would like to design things, but a typical real-world
scenario when integrating with external systems.

Before explaining how to create a cluster singleton actor, let's define message classes @java[and their corresponding factory methods]
which will be used by the singleton.

Scala
:  @@snip [ClusterSingletonManagerSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/singleton/ClusterSingletonManagerSpec.scala) { #singleton-message-classes }

Java
:  @@snip [ClusterSingletonManagerTest.java](/akka-cluster-tools/src/test/java/akka/cluster/singleton/TestSingletonMessages.java) { #singleton-message-classes }

On each node in the cluster you need to start the `ClusterSingletonManager` and
supply the `Props` of the singleton actor, in this case the JMS queue consumer.

Scala
:  @@snip [ClusterSingletonManagerSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/singleton/ClusterSingletonManagerSpec.scala) { #create-singleton-manager }

Java
:  @@snip [ClusterSingletonManagerTest.java](/akka-cluster-tools/src/test/java/akka/cluster/singleton/ClusterSingletonManagerTest.java) { #create-singleton-manager }

Here we limit the singleton to nodes tagged with the `"worker"` role, but all nodes, independent of
role, can be used by not specifying `withRole`.

We use an application specific `terminationMessage` @java[(i.e. `TestSingletonMessages.end()` message)] to be able to close the
resources before actually stopping the singleton actor. Note that `PoisonPill` is a
perfectly fine `terminationMessage` if you only need to stop the actor.

Here is how the singleton actor handles the `terminationMessage` in this example.

Scala
:  @@snip [ClusterSingletonManagerSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/singleton/ClusterSingletonManagerSpec.scala) { #consumer-end }

Java
:  @@snip [ClusterSingletonManagerTest.java](/akka-cluster-tools/src/test/java/akka/cluster/singleton/Consumer.java) { #consumer-end }

With the names given above, access to the singleton can be obtained from any cluster node using a properly
configured proxy.

Scala
:  @@snip [ClusterSingletonManagerSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/singleton/ClusterSingletonManagerSpec.scala) { #create-singleton-proxy }

Java
:  @@snip [ClusterSingletonManagerTest.java](/akka-cluster-tools/src/test/java/akka/cluster/singleton/ClusterSingletonManagerTest.java) { #create-singleton-proxy }

A more comprehensive sample is available in the tutorial named 
@scala[[Distributed workers with Akka and Scala!](https://github.com/typesafehub/activator-akka-distributed-workers)]@java[[Distributed workers with Akka and Java!](https://github.com/typesafehub/activator-akka-distributed-workers-java)].

## Configuration

For the full documentation of this feature and for new projects see @ref:[Cluster Singleton - configuration](typed/cluster-singleton.md#configuration).

## Supervision

There are two actors that could potentially be supervised. For the `consumer` singleton created above these would be: 

* Cluster singleton manager e.g. `/user/consumer` which runs on every node in the cluster
* The user actor e.g. `/user/consumer/singleton` which the manager starts on the oldest node

The Cluster singleton manager actor should not have its supervision strategy changed as it should always be running.
However it is sometimes useful to add supervision for the user actor. 
To accomplish this add a parent supervisor actor which will be used to create the 'real' singleton instance. 
Below is an example implementation (credit to [this StackOverflow answer](https://stackoverflow.com/questions/36701898/how-to-supervise-cluster-singleton-in-akka/36716708#36716708))

Scala
:  @@snip [ClusterSingletonSupervision.scala](/akka-docs/src/test/scala/docs/cluster/singleton/ClusterSingletonSupervision.scala) { #singleton-supervisor-actor }

Java
:  @@snip [SupervisorActor.java](/akka-docs/src/test/java/jdocs/cluster/singleton/SupervisorActor.java) { #singleton-supervisor-actor }

And used here

Scala
:  @@snip [ClusterSingletonSupervision.scala](/akka-docs/src/test/scala/docs/cluster/singleton/ClusterSingletonSupervision.scala) { #singleton-supervisor-actor-usage }

Java
:  @@snip [ClusterSingletonSupervision.java](/akka-docs/src/test/java/jdocs/cluster/singleton/ClusterSingletonSupervision.java) { #singleton-supervisor-actor-usage-imports }
@@snip [ClusterSingletonSupervision.java](/akka-docs/src/test/java/jdocs/cluster/singleton/ClusterSingletonSupervision.java) { #singleton-supervisor-actor-usage }

## Lease

For the full documentation of this feature and for new projects see @ref:[Cluster Singleton - Lease](typed/cluster-singleton.md#lease).
 
