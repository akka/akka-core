<a id="when-and-where-to-use-akka-cluster"></a>
# Choosing Akka Cluster

An architectural choice you have to make is if you are going to use a microservices architecture or
a traditional distributed application. This choice will influence how you should use Akka Cluster.

The [Stateful or Stateless applications: to Akka Cluster or not](https://akka.io/blog/news/2020/06/01/akka-cluster-motivation)
video is a good starting point to understand the motivation to use Akka Cluster.

## Microservices

Microservices has many attractive properties, such as the independent nature of microservices allows for
multiple smaller and more focused teams that can deliver new functionality more frequently and can
respond quicker to business opportunities. Reactive Microservices should be isolated, autonomous, and have
a single responsibility as identified by Jonas Bonér in the book
[Reactive Microsystems: The Evolution of Microservices at Scale](https://www.oreilly.com/library/view/reactive-microsystems/9781491994368/).

In a microservices architecture, you should consider communication within a service and between services.

In general we recommend against using Akka Cluster and actor messaging between _different_ services because that
would result in a too tight code coupling between the services and difficulties deploying these independent of
each other, which is one of the main reasons for using a microservices architecture.
See the discussion on @extref[Internal and External Communication](platform-guide:concepts/internal-and-external-communication.html)
for some background on this.

Nodes of a single service (collectively called a cluster) require less decoupling. They share the same code and
are deployed together, as a set, by a single team or individual. There might be two versions running concurrently
during a rolling deployment, but deployment of the entire set has a single point of control. For this reason,
intra-service communication can take advantage of Akka Cluster, failure management and actor messaging, which
is convenient to use and has great performance.

Between different services [Akka HTTP](https://doc.akka.io/libraries/akka-http/current/) or
[Akka gRPC](https://doc.akka.io/libraries/akka-grpc/current/) can be used for synchronous (yet non-blocking)
communication and [Akka Streams Kafka](https://doc.akka.io/libraries/alpakka-kafka/current/) or other
[Alpakka](https://doc.akka.io/libraries/alpakka/current/) connectors for integration asynchronous communication.
All those communication mechanisms work well with streaming of messages with end-to-end back-pressure, and the
synchronous communication tools can also be used for single request response interactions. It is also important
to note that when using these tools both sides of the communication do not have to be implemented with Akka,
nor does the programming language matter.

## Traditional distributed application

We acknowledge that microservices also introduce many new challenges and it's not the only way to
build applications. A traditional distributed application may have less complexity and work well in many cases.
For example for a small startup, with a single team, building an application where time to market is everything.
Akka Cluster can efficiently be used for building such distributed application.

In this case, you have a single deployment unit, built from a single code base (or using traditional binary
dependency management to modularize) but deployed across many nodes using a single cluster.
Tighter coupling is OK, because there is a central point of deployment and control. In some cases, nodes may
have specialized runtime roles which means that the cluster is not totally homogenous (e.g., "front-end" and
"back-end" nodes, or dedicated master/worker nodes) but if these are run from the same built artifacts this
is just a runtime behavior and doesn't cause the same kind of problems you might get from tight coupling of
totally separate artifacts.

A tightly coupled distributed application has served the industry and many Akka users well for years and is
still a valid choice.

## Distributed monolith

There is also an anti-pattern that is sometimes called "distributed monolith". You have multiple services
that are built and deployed independently from each other, but they have a tight coupling that makes this
very risky, such as a shared cluster, shared code and dependencies for service API calls, or a shared
database schema. There is a false sense of autonomy because of the physical separation of the code and
deployment units, but you are likely to encounter problems because of changes in the implementation of
one service leaking into the behavior of others.

Organizations that find themselves in this situation often react by trying to centrally coordinate deployment
of multiple services, at which point you have lost the principal benefit of microservices while taking on
the costs. You are in a halfway state with things that aren't really separable being built and deployed
in a separate way. Some people do this, and some manage to make it work, but it's not something we would
recommend and it needs to be carefully managed.
