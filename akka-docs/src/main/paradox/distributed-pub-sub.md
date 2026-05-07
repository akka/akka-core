# Classic Distributed Publish Subscribe in Cluster

@@include[includes.md](includes.md) { #actor-api }
For the new API see @ref[Distributed Publish Subscribe in Cluster](./typed/distributed-pub-sub.md) 

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Distributed Publish Subscribe you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-cluster-tools_$scala.binary.version$"
  version=AkkaVersion
}

@@project-info{ projectId="akka-cluster-tools" }

## Introduction

How do I send a message to an actor without knowing which node it is running on?

How do I send messages to all actors in the cluster that have registered interest
in a named topic?

This pattern provides a mediator actor, @apidoc[akka.cluster.pubsub.DistributedPubSubMediator](akka.cluster.pubsub.DistributedPubSubMediator),
that manages a registry of actor references and replicates the entries to peer
actors among all cluster nodes or a group of nodes tagged with a specific role.

The `DistributedPubSubMediator` actor is supposed to be started on all nodes,
or all nodes with specified role, in the cluster. The mediator can be
started with the @apidoc[DistributedPubSub] extension or as an ordinary actor.

The registry is eventually consistent, i.e. changes are not immediately visible at
other nodes, but typically they will be fully replicated to all other nodes after
a few seconds. Changes are only performed in the own part of the registry and those
changes are versioned. Deltas are disseminated in a scalable way to other nodes with
a gossip protocol.

Cluster members with status @ref:[WeaklyUp](typed/cluster-membership.md#weakly-up),
will participate in Distributed Publish Subscribe, i.e. subscribers on nodes with
`WeaklyUp` status will receive published messages if the publisher and subscriber are on
same side of a network partition.

You can send messages via the mediator on any node to registered actors on
any other node.

There a two different modes of message delivery, explained in the sections
@ref:[Publish](#distributed-pub-sub-publish) and @ref:[Send](#distributed-pub-sub-send) below.

@@@ div { .group-scala }

A more comprehensive sample is available in the
tutorial named [Akka Clustered PubSub with Scala!](https://github.com/typesafehub/activator-akka-clustering).

@@@

<a id="distributed-pub-sub-publish"></a>
## Publish

This is the true pub/sub mode. A typical usage of this mode is a chat room in an instant
messaging application.

Actors are registered to a named topic. This enables many subscribers on each node.
The message will be delivered to all subscribers of the topic.

For efficiency the message is sent over the wire only once per node (that has a matching topic),
and then delivered to all subscribers of the local topic representation.

You register actors to the local mediator with `DistributedPubSubMediator.Subscribe`.
Successful @apidoc[DistributedPubSubMediator.Subscribe] and @apidoc[DistributedPubSubMediator.Unsubscribe] is acknowledged with
@apidoc[DistributedPubSubMediator.SubscribeAck] and @apidoc[DistributedPubSubMediator.UnsubscribeAck]
replies. The acknowledgment means that the subscription is registered, but it can still
take some time until it is replicated to other nodes.

You publish messages by sending @apidoc[DistributedPubSubMediator.Publish] message to the
local mediator.

Actors are automatically removed from the registry when they are terminated, or you
can explicitly remove entries with `DistributedPubSubMediator.Unsubscribe`.

An example of a subscriber actor:

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #subscriber }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #subscriber }

Subscriber actors can be started on several nodes in the cluster, and all will receive
messages published to the "content" topic.

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #start-subscribers }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #start-subscribers }

A simple actor that publishes to this "content" topic:

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #publisher }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #publisher }

It can publish messages to the topic from anywhere in the cluster:

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #publish-message }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #publish-message }

### Topic Groups

Actors may also be subscribed to a named topic with a `group` id.
If subscribing with a group id, each message published to a topic with the
`sendOneMessageToEachGroup` flag set to `true` is delivered via the supplied @apidoc[akka.routing.RoutingLogic]
(default random) to one actor within each subscribing group.

If all the subscribed actors have the same group id, then this works just like
@apidoc[DistributedPubSubMediator.Send] and each message is only delivered to one subscriber.

If all the subscribed actors have different group names, then this works like
normal @apidoc[DistributedPubSubMediator.Publish] and each message is broadcasted to all subscribers.

@@@ note

Note that if the group id is used it is part of the topic identifier.
Messages published with `sendOneMessageToEachGroup=false` will not be delivered
to subscribers that subscribed with a group id.
Messages published with `sendOneMessageToEachGroup=true` will not be delivered
to subscribers that subscribed without a group id.

@@@

<a id="distributed-pub-sub-send"></a>
## Send

This is a point-to-point mode where each message is delivered to one destination,
but you still do not have to know where the destination is located.
A typical usage of this mode is private chat to one other user in an instant messaging
application. It can also be used for distributing tasks to registered workers, like a
cluster aware router where the routees dynamically can register themselves.

The message will be delivered to one recipient with a matching path, if any such
exists in the registry. If several entries match the path because it has been registered
on several nodes the message will be sent via the supplied @apidoc[RoutingLogic](akka.routing.RoutingLogic) (default random)
to one destination. The sender of the message can specify that local affinity is preferred,
i.e. the message is sent to an actor in the same local actor system as the used mediator actor,
if any such exists, otherwise route to any other matching entry.

You register actors to the local mediator with @apidoc[DistributedPubSubMediator.Put].
The @apidoc[actor.ActorRef] in `Put` must belong to the same local actor system as the mediator.
The path without address information is the key to which you send messages.
On each node there can only be one actor for a given path, since the path is unique
within one local actor system.

You send messages by sending @apidoc[DistributedPubSubMediator.Send] message to the
local mediator with the path (without address information) of the destination
actors.

Actors are automatically removed from the registry when they are terminated, or you
can explicitly remove entries with @apidoc[DistributedPubSubMediator.Remove].

An example of a destination actor:

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #send-destination }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #send-destination }

Destination actors can be started on several nodes in the cluster, and all will receive
messages sent to the path (without address information).

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #start-send-destinations }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #start-send-destinations }

A simple actor that sends to the path:

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #sender }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #sender }

It can send messages to the path from anywhere in the cluster:

Scala
:  @@snip [DistributedPubSubMediatorSpec.scala](/akka-cluster-tools/src/multi-jvm/scala/akka/cluster/pubsub/DistributedPubSubMediatorSpec.scala) { #send-message }

Java
:  @@snip [DistributedPubSubMediatorTest.java](/akka-cluster-tools/src/test/java/akka/cluster/pubsub/DistributedPubSubMediatorTest.java) { #send-message }

It is also possible to broadcast messages to the actors that have been registered with
@apidoc[DistributedPubSubMediator.Put]. Send @apidoc[DistributedPubSubMediator.SendToAll] message to the local mediator and the wrapped message
will then be delivered to all recipients with a matching path. Actors with
the same path, without address information, can be registered on different nodes.
On each node there can only be one such actor, since the path is unique within one
local actor system.

Typical usage of this mode is to broadcast messages to all replicas
with the same path, e.g. 3 actors on different nodes that all perform the same actions,
for redundancy. You can also optionally specify a property (`allButSelf`) deciding
if the message should be sent to a matching path on the self node or not.

## DistributedPubSub Extension

In the example above the mediator is started and accessed with the @apidoc[akka.cluster.pubsub.DistributedPubSub](akka.cluster.pubsub.DistributedPubSub) extension.
That is convenient and perfectly fine in most cases, but it can be good to know that it is possible to
start the mediator actor as an ordinary actor and you can have several different mediators at the same
time to be able to divide a large number of actors/topics to different mediators. For example you might
want to use different cluster roles for different mediators.

The `DistributedPubSub` extension can be configured with the following properties:

@@snip [reference.conf](/akka-cluster-tools/src/main/resources/reference.conf) { #pub-sub-ext-config }

It is recommended to load the extension when the actor system is started by defining it in
`akka.extensions` configuration property. Otherwise it will be activated when first used
and then it takes a while for it to be populated.

```
akka.extensions = ["akka.cluster.pubsub.DistributedPubSub"]
```

## Delivery Guarantee

As in @ref:[Message Delivery Reliability](general/message-delivery-reliability.md) of Akka, message delivery guarantee in distributed pub sub modes is **at-most-once delivery**.
In other words, messages can be lost over the wire.

If you are looking for at-least-once delivery guarantee, we recommend [Alpakka Kafka](https://doc.akka.io/libraries/alpakka-kafka/current/).
