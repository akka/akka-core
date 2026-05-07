# Cluster Singleton

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Cluster Singleton](../cluster-singleton.md).

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
  artifact=akka-cluster-typed_$scala.binary.version$
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster-typed" }

## Introduction

For some use cases it is convenient and sometimes also mandatory to ensure that
you have exactly one actor of a certain type running somewhere in the cluster.

Some examples:

 * single point of responsibility for certain cluster-wide consistent decisions, or
coordination of actions across the cluster system
 * single entry point to an external system
 * single master, many workers
 * centralized naming service, or routing logic

Using a singleton should not be the first design choice. It has several drawbacks,
such as single-point of bottleneck. Single-point of failure is also a relevant concern,
but for some cases this feature takes care of that by making sure that another singleton
instance will eventually be started.

@@@ warning

Make sure to not use a Cluster downing strategy that may split the cluster into several separate clusters in
case of network problems or system overload (long GC pauses), since that will result in in *multiple Singletons*
being started, one in each separate cluster!
See @ref:[Downing](cluster.md#downing).

@@@

### Singleton manager

The cluster singleton pattern manages one singleton actor instance among all cluster nodes or a group of nodes tagged with
a specific role. The singleton manager is an actor that is supposed to be started with @apidoc[ClusterSingleton.init](ClusterSingleton) {scala="#init[M](singleton:akka.cluster.typed.SingletonActor[M]):akka.actor.typed.ActorRef[M]" java="#init(akka.cluster.typed.SingletonActor)"} as
early as possible on all nodes, or all nodes with specified role, in the cluster. 

The actual singleton actor is

* Started on the oldest node by creating a child actor from
supplied @apidoc[Behavior](typed.Behavior). It makes sure that at most one singleton instance is running at any point in time.
* Always running on the oldest member with specified role.

The oldest member is determined by @apidoc[akka.cluster.Member#isOlderThan](cluster.Member) {scala="#isOlderThan(other:akka.cluster.Member):Boolean" java="#isOlderThan(akka.cluster.Member)"}
This can change when removing that member from the cluster. Be aware that there is a short time
period when there is no active singleton during the hand-over process.

When the oldest node is @ref:[Leaving](cluster.md#leaving) the cluster there is an exchange from the oldest
and the new oldest before a new singleton is started up.

The cluster @ref:[failure detector](cluster.md#failure-detector) will notice when oldest node becomes unreachable due to
things like JVM crash, hard shut down, or network failure. After @ref:[Downing](cluster.md#downing) and removing that
node the a new oldest node will take over and a new singleton actor is created. For these failure scenarios there will
not be a graceful hand-over, but more than one active singletons is prevented by all reasonable means. Some corner
cases are eventually resolved by configurable timeouts. Additional safety can be added by using a @ref:[Lease](#lease). 

### Singleton proxy

To communicate with a given named singleton in the cluster you can access it though a proxy @apidoc[ActorRef](typed.ActorRef).
When calling @apidoc[ClusterSingleton.init](ClusterSingleton) {scala="#init[M](singleton:akka.cluster.typed.SingletonActor[M]):akka.actor.typed.ActorRef[M]" java="#init(akka.cluster.typed.SingletonActor)"} for a given `singletonName` on a node an `ActorRef` is returned. It is
to this `ActorRef` that you can send messages to the singleton instance, independent of which node the singleton
instance is active. `ClusterSingleton.init` can be called multiple times, if there already is a singleton manager 
running on this node, no additional manager is started, and if there is one running an `ActorRef` to the proxy
is returned.
   
The proxy will route all messages to the current instance of the singleton, and keep track of
the oldest node in the cluster and discover the singleton's `ActorRef`.
There might be periods of time during which the singleton is unavailable,
e.g., when a node leaves the cluster. In these cases, the proxy will buffer the messages sent to the
singleton and then deliver them when the singleton is finally available. If the buffer is full
the proxy will drop old messages when new messages are sent via the proxy.
The size of the buffer is configurable and it can be disabled by using a buffer size of 0.

It's worth noting that messages can always be lost because of the distributed nature of these actors.
As always, additional logic should be implemented in the singleton (acknowledgement) and in the
client (retry) actors to ensure at-least-once message delivery.

The singleton instance will not run on members with status @ref:[WeaklyUp](cluster-membership.md#weaklyup-members).

## Potential problems to be aware of

This pattern may seem to be very tempting to use at first, but it has several drawbacks, some of them are listed below:

 * The cluster singleton may quickly become a *performance bottleneck*.
 * You can not rely on the cluster singleton to be *non-stop* available — e.g. when the node on which the singleton
   has been running dies, it will take a few seconds for this to be noticed and the singleton be migrated to another node.
 * If many singletons are used be aware of that all will run on the oldest node (or oldest with configured role).
   @ref:[Cluster Sharding](cluster-sharding.md) combined with keeping the "singleton" entities alive can be a better
   alternative. 

@@@ warning
 
Make sure to not use a Cluster downing strategy that may split the cluster into several separate clusters in
case of network problems or system overload (long GC pauses), since that will result in in *multiple Singletons*
being started, one in each separate cluster!
See @ref:[Downing](cluster.md#downing).
 
@@@

## Example

Any @apidoc[Behavior](typed.Behavior) can be run as a singleton. E.g. a basic counter:

Scala
:  @@snip [SingletonCompileOnlySpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/SingletonCompileOnlySpec.scala) { #counter }

Java
:  @@snip [SingletonCompileOnlyTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/SingletonCompileOnlyTest.java) { #counter }

Then on every node in the cluster, or every node with a given role, use the @apidoc[ClusterSingleton$] extension
to spawn the singleton. An instance will per data centre of the cluster:


Scala
:  @@snip [SingletonCompileOnlySpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/SingletonCompileOnlySpec.scala) { #singleton }

Java
:  @@snip [SingletonCompileOnlyTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/SingletonCompileOnlyTest.java) { #import #singleton }

## Supervision

The default @ref[supervision strategy](./fault-tolerance.md) when an exception is thrown is for an actor to be stopped. 
The above example overrides this to `restart` to ensure it is always running. Another option would be to restart with 
a backoff: 


Scala
:  @@snip [SingletonCompileOnlySpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/SingletonCompileOnlySpec.scala) { #backoff}

Java
:  @@snip [SingletonCompileOnlyTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/SingletonCompileOnlyTest.java) { #backoff}

Be aware that this means there will be times when the singleton won't be running as restart is delayed.
See @ref[Fault Tolerance](./fault-tolerance.md) for a full list of supervision options.


## Application specific stop message

An application specific `stopMessage` can be used to close the resources before actually stopping the singleton actor. 
This `stopMessage` is sent to the singleton actor to tell it to finish its work, close resources, and stop. The hand-over to the new oldest node is completed when the
singleton actor is terminated.
If the shutdown logic does not include any asynchronous actions it can be executed in the @apidoc[PostStop$] signal handler.

Scala
:  @@snip [SingletonCompileOnlySpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/SingletonCompileOnlySpec.scala) { #stop-message }

Java
:  @@snip [SingletonCompileOnlyTest.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/SingletonCompileOnlyTest.java) { #stop-message }

## Lease

A @ref[lease](../coordination.md) can be used as an additional safety measure to ensure that two singletons 
don't run at the same time. Reasons for how this can happen:

* Network partitions without an appropriate downing provider
* Mistakes in the deployment process leading to two separate Akka Clusters
* Timing issues between removing members from the Cluster on one side of a network partition and shutting them down on the other side

A lease can be a final backup that means that the singleton actor won't be created unless
the lease can be acquired. 

To use a lease for every singleton in an application set `akka.cluster.singleton.use-lease` to the configuration location
of the lease to use. A lease with the name `<actor system name>-singleton-<singleton actor path>` is used and
the owner is set to the @scala[`Cluster(system).selfAddress.hostPort`]@java[`Cluster.get(system).selfAddress().hostPort()`].

Note that the `akka.cluster.singleton.lease-name` configuration key is ignored and cannot be used to configure singleton 
lease names.

It is also possible to configure one individual singleton to use lease by defining a lease config block specifically for it:

@@snip [LeaseDocSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/LeaseDocSpec.scala) { #singleton-config }

And then setting that into @apidoc[LeaseUsageSettings] that can be set in the @apidoc[ClusterSingletonSettings]:

Scala
:  @@snip [LeaseDocSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/LeaseDocSpec.scala) { #singleton-load-config }

Java
:  @@snip [LeaseDocExample.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/LeaseDocExample.java) { #singleton-load-config }


Or programmatically specifying the lease settings:

Scala
:  @@snip [LeaseDocSpec.scala](/akka-cluster-typed/src/test/scala/docs/akka/cluster/typed/LeaseDocSpec.scala) { #singleton-settings }

Java
:  @@snip [LeaseDocExample.java](/akka-cluster-typed/src/test/java/jdocs/akka/cluster/typed/LeaseDocExample.java) { #singleton-settings }


If the cluster singleton manager can't acquire the lease it will keep retrying while it is the oldest node in the cluster.
If the lease is lost then the singleton actor will be terminated then the lease will be re-tried.

## Accessing singleton of another data centre

TODO @github[#27705](#27705)

## Configuration

The following configuration properties are read by the @apidoc[ClusterSingletonManagerSettings](singleton.ClusterSingletonManagerSettings)
when created with a @apidoc[ActorSystem](typed.ActorSystem) parameter. It is also possible to amend the `ClusterSingletonManagerSettings`
or create it from another config section with the same layout as below. `ClusterSingletonManagerSettings` is
a parameter to the @apidoc[ClusterSingletonManager.props](ClusterSingletonManager$) {scala="#props(singletonProps:akka.actor.Props,terminationMessage:Any,settings:akka.cluster.singleton.ClusterSingletonManagerSettings):akka.actor.Props" java="#props(akka.actor.Props,java.lang.Object,akka.cluster.singleton.ClusterSingletonManagerSettings)"} factory method, i.e. each singleton can be configured
with different settings if needed.

@@snip [reference.conf](/akka-cluster-tools/src/main/resources/reference.conf) { #singleton-config }

The following configuration properties are read by the @apidoc[ClusterSingletonSettings](typed.ClusterSingletonSettings)
when created with a @apidoc[ActorSystem](typed.ActorSystem) parameter. `ClusterSingletonSettings` is an optional parameter in
@apidoc[ClusterSingleton.init](ClusterSingleton) {scala="#init[M](singleton:akka.cluster.typed.SingletonActor[M]):akka.actor.typed.ActorRef[M]" java="#init(akka.cluster.typed.SingletonActor)"}. It is also possible to amend the @apidoc[ClusterSingletonProxySettings]
or create it from another config section with the same layout as below.

@@snip [reference.conf](/akka-cluster-tools/src/main/resources/reference.conf) { #singleton-proxy-config }

