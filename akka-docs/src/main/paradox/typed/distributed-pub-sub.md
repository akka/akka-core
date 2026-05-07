# Distributed Publish Subscribe in Cluster

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Distributed Publish Subscribe](../distributed-pub-sub.md).

## Module info

The distributed publish subscribe topic API is available and usable with the core `akka-actor-typed` module, however it will only be distributed
when used in a clustered application.

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-cluster-typed_$scala.binary.version$"
  version=AkkaVersion
}

## The Topic Registry

Distributed publish subscribe is achieved by representing each pub sub topic with an actor, @apidoc[akka.actor.typed.pubsub.Topic](akka.actor.typed.pubsub.Topic$).

The topic actor needs to run on each node where subscribers will live or that wants to publish messages to the topic. 

Topics can be looked up in the @apidoc[PubSub](akka.actor.typed.pubsub.PubSub$) registry, this way the same topic will be represented by the same actor for a whole
actor system. If the topic has not yet been started it is started and returned, if it already is running, the existing `ActorRef` is returned.

Scala
:  @@snip [PubSubExample.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/pubsub/PubSubExample.scala) { #lookup-topic }

Java
:  @@snip [PubSubExample.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/pubsub/PubSubExample.java) { #lookup-topic }

The identity of the topic is the topic name and if it already has been started with a different type of message this will
lead to an exception.

Local actors can then subscribe to the topic (and unsubscribe from it) via messages defined in @apidoc[Topic](akka.actor.typed.pubsub.Topic$):

Scala
:  @@snip [PubSubExample.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/pubsub/PubSubExample.scala) { #subscribe }

Java
:  @@snip [PubSubExample.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/pubsub/PubSubExample.java) { #subscribe }

And publish messages to the topic:

Scala
:  @@snip [PubSubExample.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/pubsub/PubSubExample.scala) { #publish }

Java
:  @@snip [PubSubExample.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/pubsub/PubSubExample.java) { #publish }

Messages published only be delivered to other nodes if the topic is started and there are any local subscribers registered 
with the topic there. The message is deduplicated so that even if there are multiple subscribers on a node, the message
is only passed over the network to that node once.

It is possible to define a TTL (time to live) for the local topic actor, if it has no local subscribers or messages 
passing through for the given time period it stopped and removed from the registry:

Scala
:  @@snip [PubSubExample.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/pubsub/PubSubExample.scala) { #ttl }

Java
:  @@snip [PubSubExample.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/pubsub/PubSubExample.java) { #ttl }


## The Topic Actor

The topic actor can also be started and managed manually. This means that multiple actors for the same topic can be
started on the same node. Messages published to a topic on other cluster nodes will be sent between the nodes once per
active topic actor that has any local subscribers:

Scala
:  @@snip [PubSubExample.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/pubsub/PubSubExample.scala) { #start-topic }

Java
:  @@snip [PubSubExample.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/pubsub/PubSubExample.java) { #start-topic }



## Pub Sub Scalability

Each topic is represented by one @ref[Receptionist](actor-discovery.md) service key meaning that the number of topics 
will scale to thousands or tens of thousands but for higher numbers of topics will require custom solutions. It also means
that a very high turnaround of unique topics will not work well and for such use cases a custom solution is advised.

The topic actor acts as a proxy and delegates to the local subscribers handling deduplication so that a published message
is only sent once to a node regardless of how many subscribers there are to the topic on that node.

When a topic actor has no subscribers for a topic it will deregister itself from the receptionist meaning published messages
for the topic will not be sent to it.

## Delivery Guarantee

As in @ref:[Message Delivery Reliability](../general/message-delivery-reliability.md) of Akka, message delivery guarantee in distributed pub sub modes is **at-most-once delivery**. In other words, messages can be lost over the wire. In addition to that the registry of nodes which have subscribers is eventually consistent
meaning that subscribing an actor on one node will have a short delay before it is known on other nodes and published to.

If you are looking for at-least-once delivery guarantee, we recommend [Alpakka Kafka](https://doc.akka.io/libraries/alpakka-kafka/current/).


