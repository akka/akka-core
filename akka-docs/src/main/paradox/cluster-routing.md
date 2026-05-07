# Classic Cluster Aware Routers

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[routers](typed/routers.md).

All @ref:[routers](routing.md) can be made aware of member nodes in the cluster, i.e.
deploying new routees or looking up routees on nodes in the cluster.
When a node becomes unreachable or leaves the cluster the routees of that node are
automatically unregistered from the router. When new nodes join the cluster, additional
routees are added to the router, according to the configuration. Routees are also added
when a node becomes reachable again, after having been unreachable.

Cluster aware routers make use of members with status @ref:[WeaklyUp](typed/cluster-membership.md#weakly-up) if that feature
is enabled.

There are two distinct types of routers.

 * **Group - router that sends messages to the specified path using actor selection**
The routees can be shared among routers running on different nodes in the cluster.
One example of a use case for this type of router is a service running on some backend
nodes in the cluster and used by routers running on front-end nodes in the cluster.
 * **Pool - router that creates routees as child actors and deploys them on remote nodes.**
Each router will have its own routee instances. For example, if you start a router
on 3 nodes in a 10-node cluster, you will have 30 routees in total if the router is
configured to use one instance per node. The routees created by the different routers
will not be shared among the routers. One example of a use case for this type of router
is a single master that coordinates jobs and delegates the actual work to routees running
on other nodes in the cluster.

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Cluster aware routers, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-cluster_$scala.binary.version$"
  version=AkkaVersion
}

## Router with Group of Routees

When using a `Group` you must start the routee actors on the cluster member nodes.
That is not done by the router. The configuration for a group looks like this::

```
akka.actor.deployment {
  /statsService/workerRouter {
      router = consistent-hashing-group
      routees.paths = ["/user/statsWorker"]
      cluster {
        enabled = on
        allow-local-routees = on
        use-roles = ["compute"]
      }
    }
}
```

@@@ note

The routee actors should be started as early as possible when starting the actor system, because
the router will try to use them as soon as the member status is changed to 'Up'.

@@@

The actor paths that are defined in `routees.paths` are used for selecting the
actors to which the messages will be forwarded to by the router. The path should not contain protocol and address information because they are retrieved dynamically from the cluster membership. 
Messages will be forwarded to the routees using @ref:[ActorSelection](actors.md#actorselection), so the same delivery semantics should be expected.
It is possible to limit the lookup of routees to member nodes tagged with a particular set of roles by specifying `use-roles`.

`max-total-nr-of-instances` defines total number of routees in the cluster. By default `max-total-nr-of-instances`
is set to a high value (10000) that will result in new routees added to the router when nodes join the cluster.
Set it to a lower value if you want to limit total number of routees.

The same type of router could also have been defined in code:

Scala
:  @@snip [StatsService.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsService.scala) { #router-lookup-in-code }

Java
:  @@snip [StatsService.java](/akka-docs/src/test/java/jdocs/cluster/StatsService.java) { #router-lookup-in-code }

See @ref:[reference configuration](general/configuration-reference.md#config-akka-cluster) for further descriptions of the settings.

### Router Example with Group of Routees

Let's take a look at how to use a cluster aware router with a group of routees,
i.e. router sending to the paths of the routees.

The example application provides a service to calculate statistics for a text.
When some text is sent to the service it splits it into words, and delegates the task
to count number of characters in each word to a separate worker, a routee of a router.
The character count for each word is sent back to an aggregator that calculates
the average number of characters per word when all results have been collected.

Messages:

Scala
:  @@snip [StatsMessages.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsMessages.scala) { #messages }

Java
:  @@snip [StatsMessages.java](/akka-docs/src/test/java/jdocs/cluster/StatsMessages.java) { #messages }

The worker that counts number of characters in each word:

Scala
:  @@snip [StatsWorker.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsWorker.scala) { #worker }

Java
:  @@snip [StatsWorker.java](/akka-docs/src/test/java/jdocs/cluster/StatsWorker.java) { #worker }

The service that receives text from users and splits it up into words, delegates to workers and aggregates:

@@@ div { .group-scala }

@@snip [StatsService.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsService.scala) { #service }

@@@

@@@ div { .group-java }

@@snip [StatsService.java](/akka-docs/src/test/java/jdocs/cluster/StatsService.java) { #service }
@@snip [StatsAggregator.java](/akka-docs/src/test/java/jdocs/cluster/StatsAggregator.java) { #aggregator }

@@@

Note, nothing cluster specific so far, just plain actors.

All nodes start `StatsService` and `StatsWorker` actors. Remember, routees are the workers in this case.
The router is configured with `routees.paths`::

```
akka.actor.deployment {
  /statsService/workerRouter {
    router = consistent-hashing-group
    routees.paths = ["/user/statsWorker"]
    cluster {
      enabled = on
      allow-local-routees = on
      use-roles = ["compute"]
    }
  }
}
```

This means that user requests can be sent to `StatsService` on any node and it will use
`StatsWorker` on all nodes.
 
## Router with Pool of Remote Deployed Routees

When using a `Pool` with routees created and deployed on the cluster member nodes
the configuration for a router looks like this::

```
akka.actor.deployment {
  /statsService/singleton/workerRouter {
      router = consistent-hashing-pool
      cluster {
        enabled = on
        max-nr-of-instances-per-node = 3
        allow-local-routees = on
        use-roles = ["compute"]
      }
    }
}
```

It is possible to limit the deployment of routees to member nodes tagged with a particular set of roles by
specifying `use-roles`.

`max-total-nr-of-instances` defines total number of routees in the cluster, but the number of routees
per node, `max-nr-of-instances-per-node`, will not be exceeded. By default `max-total-nr-of-instances`
is set to a high value (10000) that will result in new routees added to the router when nodes join the cluster.
Set it to a lower value if you want to limit total number of routees.

The same type of router could also have been defined in code:

Scala
:  @@snip [StatsService.scala](/akka-cluster-metrics/src/multi-jvm/scala/akka/cluster/metrics/sample/StatsService.scala) { #router-deploy-in-code }

Java
:  @@snip [StatsService.java](/akka-docs/src/test/java/jdocs/cluster/StatsService.java) { #router-deploy-in-code }

See @ref:[reference configuration](general/configuration-reference.md#config-akka-cluster) for further descriptions of the settings.

When using a pool of remote deployed routees you must ensure that all parameters of the `Props` can
be @ref:[serialized](serialization.md).

### Router Example with Pool of Remote Deployed Routees

Let's take a look at how to use a cluster aware router on single master node that creates
and deploys workers. To keep track of a single master we use the @ref:[Cluster Singleton](cluster-singleton.md)
in the cluster-tools module. The `ClusterSingletonManager` is started on each node:

Scala
:   @@@vars
    ```
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props[StatsService],
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system).withRole("compute")),
      name = "statsService")
    ```
    @@@

Java
:  @@snip [StatsSampleOneMasterMain.java](/akka-docs/src/test/java/jdocs/cluster/StatsSampleOneMasterMain.java) { #create-singleton-manager }

We also need an actor on each node that keeps track of where current single master exists and
delegates jobs to the `StatsService`. That is provided by the `ClusterSingletonProxy`:

Scala
:   @@@vars
    ```
    system.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = "/user/statsService",
        settings = ClusterSingletonProxySettings(system).withRole("compute")),
      name = "statsServiceProxy")
    ```
    @@@

Java
:  @@snip [StatsSampleOneMasterMain.java](/akka-docs/src/test/java/jdocs/cluster/StatsSampleOneMasterMain.java) { #singleton-proxy }

The `ClusterSingletonProxy` receives text from users and delegates to the current `StatsService`, the single
master. It listens to cluster events to lookup the `StatsService` on the oldest node.

All nodes start `ClusterSingletonProxy` and the `ClusterSingletonManager`. The router is now configured like this::

```
akka.actor.deployment {
  /statsService/singleton/workerRouter {
    router = consistent-hashing-pool
    cluster {
      enabled = on
      max-nr-of-instances-per-node = 3
      allow-local-routees = on
      use-roles = ["compute"]
    }
  }
}
```
The easiest way to run **Router Example with Pool of Routees** example yourself is to try the
@scala[[Akka Cluster Sample with Scala](../attachments/akka-samples-cluster-scala.zip)]@java[[Akka Cluster Sample with Java](../attachments/akka-samples-cluster-java.zip)].
It contains instructions on how to run the **Router Example with Pool of Routees** sample.
