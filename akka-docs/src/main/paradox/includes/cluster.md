<!--- #cluster-singleton --->
### Cluster Singleton

For some use cases it is convenient or necessary to ensure only one 
actor of a certain type is running somewhere in the cluster.
This can be implemented by subscribing to member events, but there are several corner
cases to consider. Therefore, this specific use case is covered by the Cluster Singleton.

<!--- #cluster-singleton --->

<!--- #cluster-sharding --->
### Cluster Sharding

Distributes actors across several nodes in the cluster and supports interaction
with the actors using their logical identifier, but without having to care about
their physical location in the cluster.

<!--- #cluster-sharding --->

<!--- #cluster-ddata --->
### Distributed Data

Distributed Data is useful when you need to share data between nodes in an
Akka Cluster. The data is accessed with an actor providing a key-value store like API.

<!--- #cluster-ddata --->
 
<!--- #cluster-pubsub --->
### Distributed Publish Subscribe

Publish-subscribe messaging between actors in the cluster based on a topic, 
i.e. the sender does not have to know on which node the destination actor is running.

<!--- #cluster-pubsub --->

<!--- #cluster-router --->
### Cluster aware routers

Distribute messages to actors on different nodes in the cluster with routing strategies
like round-robin and consistent hashing.

<!--- #cluster-router --->

<!--- #reliable-delivery --->
### Reliable Delivery

Reliable delivery and flow control of messages between actors in the Cluster.

<!--- #reliable-delivery --->

<!--- #sharding-persistence-mode-deprecated --->
@@@ warning

Persistence for state store mode is deprecated. It is recommended to migrate to `ddata` for the coordinator state and if using replicated entities
migrate to `eventsourced` for the replicated entities state.

The data written by the deprecated `persistence` state store mode for remembered entities can be read by the new remember entities `eventsourced` mode.

Once you've migrated you can not go back to `persistence` mode.

@@@
<!--- #sharding-persistence-mode-deprecated --->
