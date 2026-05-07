# Example projects

The following example projects can be downloaded. They contain build files and have instructions
of how to run.

## Quickstart

* Scala [akka-quickstart-scala.zip](../attachments/akka-quickstart-scala.zip)
* Java [akka-quickstart-java.zip](../attachments/akka-quickstart-java.zip)
 
The *Quickstart* sample is described in @ref[Introduction to Actors](../typed/actors.md#first-example) and walks you through example code that introduces how to define actor systems, actors, and
messages.

## FSM

* Scala [Dining hackers with FSM](https://github.com/akka/akka-core/tree/main/samples/akka-sample-fsm-scala) ([akka-sample-fsm-scala.zip](../attachments/akka-sample-fsm-scala.zip))
* Java [Dining hackers with FSM](https://github.com/akka/akka-core/tree/main/samples/akka-sample-fsm-java) ([akka-sample-fsm-java.zip](../attachments/akka-sample-fsm-java.zip))

This project contains a Dining Hakkers sample illustrating how to model a Finite State Machine (FSM) with actors.

## Cluster

* Scala [Cluster example project](https://github.com/akka/akka-core/tree/main/samples/akka-sample-cluster-scala#readme) ([akka-sample-cluster-scala.zip](../attachments/akka-sample-cluster-scala.zip))
* Java [Cluster example project](https://github.com/akka/akka-core/tree/main/samples/akka-sample-cluster-java#readme) ([akka-sample-cluster-java.zip](../attachments/akka-sample-cluster-java.zip))

This project contains samples illustrating different Cluster features, such as
subscribing to cluster membership events, and sending messages to actors running on nodes in the cluster
with Cluster aware routers.

It also includes Multi JVM Testing with the `sbt-multi-jvm` plugin.

## Distributed Data

* Scala [Distributed data example project](https://github.com/akka/akka-core/tree/main/samples/akka-sample-distributed-data-scala#readme) ([akka-sample-distributed-data-scala.zip](../attachments/akka-sample-distributed-data-scala.zip))
* Java [Distributed data example project](https://github.com/akka/akka-core/tree/main/samples/akka-sample-distributed-data-java#readme) ([akka-sample-distributed-data-java.zip](../attachments/akka-sample-distributed-data-java.zip))

This project contains several samples illustrating how to use Distributed Data.

## Cluster Sharding

* Scala [Cluster Sharding example](https://github.com/akka/akka-core/tree/main/samples/akka-sample-sharding-scala#readme) ([akka-sample-sharding-scala.zip](../attachments/akka-sample-sharding-scala.zip))
* Java [Cluster Sharding example](https://github.com/akka/akka-core/tree/main/samples/akka-sample-sharding-java#readme) ([akka-sample-sharding-java.zip](../attachments/akka-sample-sharding-java.zip))

This project contains a KillrWeather sample illustrating how to use Cluster Sharding.

## Persistence and CQRS

The @extref[Microservices with Akka tutorial](platform-guide:microservices-tutorial/) contains a
Shopping Cart sample illustrating how to use Event Sourcing and Projections together. The events are
consumed by even processors to build other representations from the events, or publish the events
to other services.

## Replicated Event Sourcing

The @extref[Akka Distributed Cluster Guide](akka-distributed-cluster:guide/3-active-active.html) illustrates how to use @ref:[Replicated Event Sourcing](../typed/replicated-eventsourcing.md) that supports
active-active persistent entities.

## Kafka to Cluster Sharding 

* Scala [akka-sample-kafka-to-sharding-scala.zip](../attachments/akka-sample-kafka-to-sharding-scala.zip)

This project demonstrates how to use the External Shard Allocation strategy to co-locate the consumption of Kafka
partitions with the shard that processes the messages.


