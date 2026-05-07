This tutorial contains 3 samples illustrating different [Akka cluster](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html) features.

- Subscribe to cluster membership events
- Sending messages to actors running on nodes in the cluster
- Cluster aware routers

## A Simple Cluster Example

To try this example locally, download the sources files with [akka-samples-cluster-java.zip](https://doc.akka.io/libraries/akka-core/current//attachments/akka-samples-cluster-java.zip).

Open [application.conf](src/main/resources/application.conf)

To enable cluster capabilities in your Akka project you should, at a minimum, add the remote settings, and use `cluster` as the `akka.actor.provider`. The `akka.cluster.seed-nodes` should normally also be added to your `application.conf` file.

The seed nodes are configured contact points which newly started nodes will try to connect with in order to join the cluster.

Note that if you are going to start the nodes on different machines you need to specify the ip-addresses or host names of the machines in `application.conf` instead of `127.0.0.1`.

Open [SimpleClusterApp.java](src/main/java/sample/cluster/simple/App.java).

The small program together with its configuration starts an ActorSystem with the Cluster enabled. It joins the cluster and starts an actor that logs some membership events. Take a look at the [SimpleClusterListener.java](src/main/java/sample/cluster/simple/ClusterListener.java) actor.

You can read more about the cluster concepts in the [documentation](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html).

To run this sample, first make sure everything is compiled using `mvn compile`, then run using `mvn exec:java -Dexec.mainClass="sample.cluster.simple.App"`.

`sample.cluster.simple.App` starts three actor systems (cluster members) in the same JVM process. It can be more interesting to run them in separate processes. Stop the application and then open three terminal windows.

In the first terminal window, start the first seed node with the following command:

    mvn exec:java -Dexec.mainClass="sample.cluster.simple.App" -Dexec.args=25251

25251 corresponds to the port of the first seed-nodes element in the configuration. In the log output you see that the cluster node has been started and changed status to 'Up'.

In the second terminal window, start the second seed node with the following command:

    mvn exec:java -Dexec.mainClass="sample.cluster.simple.App" -Dexec.args=25252

25252 corresponds to the port of the second seed-nodes element in the configuration. In the log output you see that the cluster node has been started and joins the other seed node and becomes a member of the cluster. Its status changed to 'Up'.

Switch over to the first terminal window and see in the log output that the member joined.

Start another node in the third terminal window with the following command:

    mvn exec:java -Dexec.mainClass="sample.cluster.simple.App" -Dexec.args=0

Now you don't need to specify the port number, 0 means that it will use a random available port. It joins one of the configured seed nodes. Look at the log output in the different terminal windows.

Start even more nodes in the same way, if you like.

Shut down one of the nodes by pressing 'ctrl-c' in one of the terminal windows. It will cause the node to do a graceful leave from the cluster, telling the other nodes in the cluster that it is leaving. It will then be removed from the cluster, which you can see in the log output in the other terminals.

Look at the source code of the actor again. It registers itself as subscriber of certain cluster events. It gets notified a stream of events leading up to the current state. After that it receives events for changes that happen in the cluster.

Now we have seen how to subscribe to cluster membership events. You can read more about it in the [documentation](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html#cluster-subscriptions). The membership events show us the state of the cluster but it does not help with accessing actors on other nodes the cluster. To do that we need to use the [Receptionist](https://doc.akka.io/libraries/akka-core/current/typed/actor-discovery.html#receptionist).

## Worker registration example

The `Receptionist` is a service registry that will work both when in single JVM apps not using cluster, and in clustered apps. 
`ActorRef`s are registered to the receptionist using a `ServiceKey`. The service key is defined with a type of message that actors registered for it will accept and a string identifier.  

Let's take a look at an example that illustrates how workers, here only on nodes with the role *backend*, register themselves to the receptionist so that *frontend* nodes will know what workers are available to perform their work. Note that a node could potentially have both roles, since the node roles are a set. The `main` provided only allows one role though. 

The example application provides a service to transform text. At a periodic interval the frontend simulates an external request to process a text which it forwards to available workers if there are any. 

Since the discovery of workers is dynamic both *backend*  and *frontend* nodes can be added to the cluster dynamically. 

The backend worker that performs the transformation job is defined in [Worker.java](src/main/java/sample/cluster/transformation/Worker.java). When starting up a worker registers itself to the receptionist so that it can be discovered through its `ServiceKey` on any node in the cluster.

The frontend that simulates user jobs as well as keeping track of available workers is defined in [Frontend.java](src/main/java/sample/cluster/transformation/Frontend.java). The actor subscribes to the `Receptionist` with the `WorkerServiceKey` to receive updates when the set of available workers in the cluster changes. If a worker dies or its node is removed from the cluster the receptionist will send out an updated listing so the frontend does not need to `watch` the workers.

To run this sample, make sure you have shut down any previously started cluster sample, then type `mvn exec:java -Dexec.mainClass="sample.cluster.transformation.App"`.

TransformationApp starts 5 actor systems (cluster members) in the same JVM process. It can be more interesting to run them in separate processes. Stop the application and run the following commands in separate terminal windows.

    mvn exec:java -Dexec.mainClass="sample.cluster.transformation.App" -Dexec.args="backend 25251"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.transformation.App" -Dexec.args="backend 25252"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.transformation.App" -Dexec.args="backend 0"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.transformation.App" -Dexec.args="frontend 0"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.transformation.App" -Dexec.args="frontend 0"

There is a component built into Akka that performs the task of subscribing to the receptionist and keeping track of available actors significantly simplifying such interactions: the group router. Let's look into how we can use those in the next section!

## Cluster Aware Routers

The [group routers](https://doc.akka.io/libraries/akka-core/current/typed/routers.html#group-router) relies on the `Receptionist` and will therefore route messages to services registered in any node of the cluster.

Let's take a look at a few samples that make use of cluster aware routers.

## Cluster routing example

Let's take a look at two different ways to distribute work across a cluster using routers. 

Note that the samples just shows off various parts of Akka Cluster and does not provide a complete structure to build a resilient distributed application with.

### Example with Group of routees

The example application provides a service to calculate statistics for a text. When some text is sent to the service it splits it into words, and delegates the task to count number of characters in each word to a separate worker, a routee of a router. The character count for each word is sent back to an aggregator that calculates the average number of characters per word when all results have been collected.

The worker that counts number of characters in each word is defined in [StatsWorker.java](src/main/java/sample/cluster/stats/StatsWorker.java).

The service that receives text from users and splits it up into words, delegates to a pool of workers and aggregates the result is defined in [StatsService.java](src/main/java/sample/cluster/stats/StatsService.java).

Note, nothing cluster specific so far, just plain actors.

Nodes in the cluster can be marked with roles, to perform different tasks, in our case we use `compute` as a role to
designate cluster nodes that should do processing of word statistics. 

In [StatsSample.java](src/main/java/sample/cluster/stats/App.java) each `compute` node starts a `StatsService`
that distributes work over N local `StatsWorkers`. The client nodes then message the `StatsService` instances through a `group` router.
The router finds services by subscribing to the cluster receptionist and a service key. Each worker is registered to the receptionist
when started. 

With this design a single `compute` node crashing will only lose the ongoing work in that node and have the other nodes
keep on with their work, but there is no single place to ask for a list of the current work in progress. 

To run the sample, type `mvn exec:java -Dexec.mainClass="sample.cluster.stats.App"` if it is not already started.

StatsSample starts 4 actor systems (cluster members) in the same JVM process. It can be more interesting to run them in separate processes. Stop the application and run the following commands in separate terminal windows.

    mvn exec:java -Dexec.mainClass="sample.cluster.stats.App" -Dexec.args="compute 25251"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.stats.App" -Dexec.args="compute 25252"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.stats.App" -Dexec.args="compute 0"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.stats.App" -Dexec.args="client 0"


### Router example with Cluster Singleton 

[AppOneMaster.java](src/main/java/sample/cluster/stats/AppOneMaster.java) each `compute` node starts 
N workers, that register themselves with the receptionist. The `StatsService` is run in a single instance in the cluster
through the Akka Cluster Singleton. The actual work is performed by workers on all compute nodes though. The workers
are reached through a group router used by the singleton. 
  
With this design it would be possible to query the singleton for current work - it knows all current requests in flight 
and could potentially make decisions based on knowing exactly what work is currently in progress. 

If the singleton node crashes however, all ongoing work is lost though since the state of the singleton is not persistent, when it is started on a new node the `StatsService` will not know of any previous work. It also means that since all work has to go through the singleton it could be come a bottleneck. If one of the other nodes crash only the ongoing work sent to them is lost, however since each ongoing request could be handled by multiple different workers on different nodes a crash could cause problems to many requests.

To run this sample, type `mvn exec:java -Dexec.mainClass="sample.cluster.stats.AppOneMaster"` if it is not already started.

AppOneMaster starts 4 actor systems (cluster members) in the same JVM process. It can be more interesting to run them in separate processes. Stop the application and run the following commands in separate terminal windows.

    mvn exec:java -Dexec.mainClass="sample.cluster.stats.AppOneMaster" -Dexec.args="compute 25251"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.stats.AppOneMaster" -Dexec.args="compute 25252"

    mvn exec:java -Dexec.mainClass="sample.cluster.stats.AppOneMaster" -Dexec.args="compute 0"
    
    mvn exec:java -Dexec.mainClass="sample.cluster.stats.AppOneMaster" -Dexec.args="client 0"
    
## Tests

The multi-jvm testkit which allows for starting a cluster with multiple separate JVMs is only available from Scala with
the Scala build tool `sbt`. Such tests are included for completeness and can be found in [src/multi-jvm](src/multi-jvm). 
You can run them by typing `sbt multi-jvm:test`.

---

The Akka family of projects is managed by teams at Lightbend with help from the community.

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).
