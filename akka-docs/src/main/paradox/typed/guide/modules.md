# Overview of Akka libraries and modules

Before delving into some best practices for writing actors, it will be helpful to preview the most commonly used Akka libraries. This will help you start thinking about the functionality you want to use in your system. All core Akka functionality is source available. Lightbend sponsors Akka development but can also help you with [commercial offerings ](https://akka.io) such as training, consulting, support, and [Enterprise capabilities](https://akka.io/pricing/do-it-yourself) &#8212; a comprehensive set of tools for managing Akka systems.

The following capabilities are included with Akka and are introduced later on this page:

* @ref:[Actor library](#actor-library)
* @ref:[Remoting](#remoting)
* @ref:[Cluster](#cluster)
* @ref:[Cluster Sharding](#cluster-sharding)
* @ref:[Cluster Singleton](#cluster-singleton)
* @ref:[Persistence](#persistence)
* @ref:[Projections](#projections)
* @ref:[Distributed Data](#distributed-data)
* @ref:[Streams](#streams)
* @ref:[Alpakka](#alpakka)
* @ref:[HTTP](#http)
* @ref:[gRPC](#grpc)
* [Other Akka modules](https://doc.akka.io/libraries/akka-core/current/common/other-modules.html)

This page does not list all available modules, but overviews the main functionality and gives you an idea of the level of sophistication you can reach when you start building systems on top of Akka.

### Actor library

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-actor-typed_$scala.binary.version$
  version=AkkaVersion
}

The core Akka library is `akka-actor-typed`, but actors are used across Akka libraries, providing a consistent, integrated model that relieves you from individually
solving the challenges that arise in concurrent or distributed system design. From a birds-eye view,
actors are a programming paradigm that takes encapsulation, one of the pillars of OOP, to its extreme.
Unlike objects, actors encapsulate not only their
state but their execution. Communication with actors is not via method calls but by passing messages. While this
difference may seem minor, it is actually what allows us to break clean from the limitations of OOP when it
comes to concurrency and remote communication. Don’t worry if this description feels too high level to fully grasp
yet, in the next chapter we will explain actors in detail. For now, the important point is that this is a model that
handles concurrency and distribution at the fundamental level instead of ad hoc patched attempts to bring these
features to OOP.

Challenges that actors solve include the following:

* How to build and design high-performance, concurrent applications.
* How to handle errors in a multi-threaded environment.
* How to protect my project from the pitfalls of concurrency.

### Remoting

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-remote_$scala.binary.version$
  version=AkkaVersion
}

Remoting enables actors that live on different computers to seamlessly exchange messages.
While distributed as a JAR artifact, Remoting resembles a module more than it does a library. You enable it mostly
with configuration and it has only a few APIs. Thanks to the actor model, a remote and local message send looks exactly the
same. The patterns that you use on local systems translate directly to remote systems.
You will rarely need to use Remoting directly, but it provides the foundation on which the Cluster subsystem is built.

Challenges Remoting solves include the following:

* How to address actor systems living on remote hosts.
* How to address individual actors on remote actor systems.
* How to turn messages to bytes on the wire.
* How to manage low-level, network connections (and reconnections) between hosts, detect crashed actor systems and hosts,
  all transparently.
* How to multiplex communications from an unrelated set of actors on the same network connection, all transparently.

### Cluster

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-typed_$scala.binary.version$
  version=AkkaVersion
}

If you have a set of actor systems that cooperate to solve some business problem, then you likely want to manage these set of
systems in a disciplined way. While Remoting solves the problem of addressing and communicating with components of
remote systems, Clustering gives you the ability to organize these into a "meta-system" tied together by a membership
protocol. **In most cases, you want to use the Cluster module instead of using Remoting directly.**
Clustering provides an additional set of services on top of Remoting that most real world applications need.

Challenges the Cluster module solves include the following:

* How to maintain a set of actor systems (a cluster) that can communicate with each other and consider each other as part of the cluster.
* How to introduce a new system safely to the set of already existing members.
* How to reliably detect systems that are temporarily unreachable.
* How to remove failed hosts/systems (or scale down the system) so that all remaining members agree on the remaining subset of the cluster.
* How to distribute computations among the current set of members.
* How to designate members of the cluster to a certain role, in other words, to provide certain services and not others.

### Cluster Sharding

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-sharding-typed_$scala.binary.version$
  version=AkkaVersion
}

Sharding helps to solve the problem of distributing a set of actors among members of an Akka cluster.
Sharding is a pattern that mostly used together with Persistence to balance a large set of persistent entities
(backed by actors) to members of a cluster and also migrate them to other nodes when members crash or leave.

Challenges that Sharding solves include the following:

* How to model and scale out a large set of stateful entities on a set of systems.
* How to ensure that entities in the cluster are distributed properly so that load is properly balanced across the machines.
* How to ensure migrating entities from a crashed system without losing the state.
* How to ensure that an entity does not exist on multiple systems at the same time and hence keeps consistent.

### Cluster Singleton

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-singleton_$scala.binary.version$
  version=AkkaVersion
}

A common (in fact, a bit too common) use case in distributed systems is to have a single entity responsible
for a given task which is shared among other members of the cluster and migrated if the host system fails.
While this undeniably introduces a common bottleneck for the whole cluster that limits scaling,
there are scenarios where the use of this pattern is unavoidable. Cluster singleton allows a cluster to select an
actor system which will host a particular actor while other systems can always access said service independently from
where it is.

The Singleton module can be used to solve these challenges:

* How to ensure that only one instance of a service is running in the whole cluster.
* How to ensure that the service is up even if the system hosting it currently crashes or shuts down during the process of scaling down.
* How to reach this instance from any member of the cluster assuming that it can migrate to other systems over time.

### Persistence

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-persistence-typed_$scala.binary.version$
  version=AkkaVersion
}

Just like objects in OOP, actors keep their state in volatile memory. Once the system is shut down, gracefully or
because of a crash, all data that was in memory is lost. Persistence provides patterns to enable actors to persist
events that lead to their current state. Upon startup, events can be replayed to restore the state of the entity hosted
by the actor. The event stream can be queried and fed into additional processing pipelines (an external Big Data
cluster for example) or alternate views (like reports).

Persistence tackles the following challenges:

* How to restore the state of an entity/actor when system restarts or crashes.
* How to implement a [CQRS system](https://docs.microsoft.com/en-us/previous-versions/msp-n-p/jj591573%28v=pandp.10%29).
* How to ensure reliable delivery of messages in face of network errors and system crashes.
* How to introspect domain events that have led an entity to its current state.
* How to leverage [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) in your application to support long-running processes while the project continues to evolve.

### Projections

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-projection-core_$scala.binary.version$
  version=AkkaVersion
}

Projections provides a simple API for consuming a stream of events for projection into a variety of downstream options.  The core dependency provides only the API and other provider dependencies are required for different source and sink implementations.

Challenges Projections solve include the following:

* Constructing alternate or aggregate views over an event stream.
* Propagating an event stream onto another downstream medium such as a Kafka topic.  
* A simple way of building read-side projections in the context of [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) and [CQRS system](https://docs.microsoft.com/en-us/previous-versions/msp-n-p/jj591573%28v=pandp.10%29)


### Distributed Data

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-typed_$scala.binary.version$
  version=AkkaVersion
}

In situations where eventual consistency is acceptable, it is possible to share data between nodes in
an Akka Cluster and accept both reads and writes even in the face of cluster partitions. This can be
achieved using [Conflict Free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) (CRDTs), where writes on different nodes can
happen concurrently and are merged in a predictable way afterward. The Distributed Data module
provides infrastructure to share data and a number of useful data types.

Distributed Data is intended to solve the following challenges:

* How to accept writes even in the face of cluster partitions.
* How to share data while at the same time ensuring low-latency local read and write access.

### Streams

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-stream-typed_$scala.binary.version$
  version=AkkaVersion
}

Actors are a fundamental model for concurrency, but there are common patterns where their use requires the user
to implement the same pattern over and over. Very common is the scenario where a chain, or graph, of actors, need to
process a potentially large, or infinite, stream of sequential events and properly coordinate resource usage so that
faster processing stages do not overwhelm slower ones in the chain or graph. Streams provide a higher-level
abstraction on top of actors that simplifies writing such processing networks, handling all the fine details in the
background and providing a safe, typed, composable programming model. Streams is also an implementation
of the [Reactive Streams standard](https://www.reactive-streams.org) which enables integration with all third
party implementations of that standard.

Streams solve the following challenges:

* How to handle streams of events or large datasets with high performance, exploiting concurrency and keeping resource usage tight.
* How to assemble reusable pieces of event/data processing into flexible pipelines.
* How to connect asynchronous services in a flexible way to each other with high performance.
* How to provide or consume Reactive Streams compliant interfaces to interface with a third party library.

### Alpakka

[Alpakka](https://doc.akka.io/libraries/alpakka/current/) is a separate module from Akka.

Alpakka is collection of modules built upon the Streams API to provide Reactive Stream connector
implementations for a variety of technologies common in the cloud and infrastructure landscape.  
See the [Alpakka overview page](https://doc.akka.io/libraries/alpakka/current/overview.html) for more details on the API and the implementation modules available.

Alpakka helps solve the following challenges:

* Connecting various infrastructure or persistence components to Stream based flows.
* Connecting to legacy systems in a manner that adheres to a Reactive Streams API.

### HTTP

[Akka HTTP](https://doc.akka.io/libraries/akka-http/current/) is a separate module from Akka.

The de facto standard for providing APIs remotely, internal or external, is [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol). Akka provides a library to construct or consume such HTTP services by giving a set of tools to create HTTP services (and serve them) and a client that can be
used to consume other services. These tools are particularly suited to streaming in and out a large set of data or real-time events by leveraging the underlying model of Akka Streams.

Some of the challenges that HTTP tackles:

* How to expose services of a system or cluster to the external world via an HTTP API in a performant way.
* How to stream large datasets in and out of a system using HTTP.
* How to stream live events in and out of a system using HTTP.

### gRPC

[Akka gRPC](https://doc.akka.io/libraries/akka-grpc/current/index.html) is a separate module from Akka.

This library provides an implementation of gRPC that integrates nicely with the @ref:[HTTP](#http) and @ref:[Streams](#streams) modules.  It is capable of generating both client and server-side artifacts from protobuf service definitions, which can then be exposed using Akka HTTP, and handled using Streams.

Some of the challenges that Akka gRPC tackles:

* Exposing services with all the benefits of gRPC & protobuf:  
  * Schema-first contract
  * Schema evolution support
  * Efficient binary protocol
  * First-class streaming support
  * Wide interoperability
  * Use of HTTP/2 connection multiplexing

### Example of module use

Akka modules integrate together seamlessly. For example, think of a large set of stateful business objects, such as documents or shopping carts, that website users access. If you model these as sharded entities, using Sharding and Persistence, they will be balanced across a cluster that you can scale out on-demand. They will be available during spikes that come from advertising campaigns or before holidays will be handled, even if some systems crash. You can also take the real-time stream of domain events with Persistence Query and use Streams to pipe them into a streaming Fast Data engine. Then, take the output of that engine as a Stream, manipulate it using Akka Streams
operators and expose it as web socket connections served by a load balanced set of HTTP servers hosted by your cluster
to power your real-time business analytics tool.

We hope this preview caught your interest! The next topic introduces the example application we will build in the tutorial portion of this guide.
