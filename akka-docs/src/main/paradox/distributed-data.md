# Classic Distributed Data

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[Distributed Data](typed/distributed-data.md).
 
## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Distributed Data, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-distributed-data_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

For the full documentation of this feature and for new projects see @ref:[Distributed Data - Introduction](typed/distributed-data.md#introduction).

## Using the Replicator

The @apidoc[akka.cluster.ddata.Replicator] actor provides the API for interacting with the data.
The `Replicator` actor must be started on each node in the cluster, or group of nodes tagged
with a specific role. It communicates with other `Replicator` instances with the same path
(without address) that are running on other nodes . For convenience it can be used with the
@apidoc[akka.cluster.ddata.DistributedData] extension but it can also be started as an ordinary
actor using the @apidoc[Replicator.props](akka.cluster.ddata.Replicator$) {scala="#props(settings:akka.cluster.ddata.ReplicatorSettings):akka.actor.Props" java="#props(akka.cluster.ddata.ReplicatorSettings)"}. If it is started as an ordinary actor it is important
that it is given the same name, started on same path, on all nodes.

Cluster members with status @ref:[WeaklyUp](typed/cluster-membership.md#weakly-up),
will participate in Distributed Data. This means that the data will be replicated to the
`WeaklyUp` nodes with the background gossip protocol. Note that it
will not participate in any actions where the consistency mode is to read/write from all
nodes or the majority of nodes. The `WeaklyUp` node is not counted
as part of the cluster. So 3 nodes + 5 `WeaklyUp` is essentially a
3 node cluster as far as consistent actions are concerned.

Below is an example of an actor that schedules tick messages to itself and for each tick
adds or removes elements from a @apidoc[ORSet](akka.cluster.ddata.ORSet) (observed-remove set). It also subscribes to
changes of this.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #data-bot }

Java
: @@snip [DataBot.java](/akka-docs/src/test/java/jdocs/ddata/DataBot.java) { #data-bot }

<a id="replicator-update"></a>
### Update

For the full documentation of this feature and for new projects see @ref:[Distributed Data - Update](typed/distributed-data.md#update).

To modify and replicate a data value you send a @apidoc[Replicator.Update](akka.cluster.ddata.Replicator.Update) message to the local
@apidoc[akka.cluster.ddata.Replicator].

The current data value for the `key` of the `Update` is passed as parameter to the @scala[@scaladoc[modify](akka.cluster.ddata.Replicator.Update#modify:Option[A]=%3EA)]@java[@javadoc[modify()](akka.cluster.ddata.Replicator.Update#modify())]
function of the `Update`. The function is supposed to return the new value of the data, which
will then be replicated according to the given consistency level.

The `modify` function is called by the `Replicator` actor and must therefore be a pure
function that only uses the data parameter and stable fields from enclosing scope. It must
for example not access the sender (@scala[@scaladoc[sender()](akka.actor.Actor#sender():akka.actor.ActorRef)]@java[@javadoc[getSender()](akka.actor.AbstractActor#getSender())]) reference of an enclosing actor.

`Update` is intended to only be sent from an actor running in same local @apidoc[akka.actor.ActorSystem]
 as the `Replicator`, because the `modify` function is typically not serializable.
 
Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #update }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #update }

As reply of the `Update` a @apidoc[Replicator.UpdateSuccess](akka.cluster.ddata.Replicator.UpdateSuccess) is sent to the sender of the
`Update` if the value was successfully replicated according to the supplied 
@ref:[write consistency level](typed/distributed-data.md#write-consistency) within the supplied timeout. Otherwise a @apidoc[Replicator.UpdateFailure](akka.cluster.ddata.Replicator.UpdateFailure) subclass is
sent back. Note that a @apidoc[Replicator.UpdateTimeout](akka.cluster.ddata.Replicator.UpdateTimeout) reply does not mean that the update completely failed
or was rolled back. It may still have been replicated to some nodes, and will eventually
be replicated to all nodes with the gossip protocol.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #update-response1 }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #update-response1 }


Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #update-response2 }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #update-response2 }

You will always see your own writes. For example if you send two @apidoc[akka.cluster.ddata.Replicator.Update] messages
changing the value of the same `key`, the `modify` function of the second message will
see the change that was performed by the first `Update` message.

It is possible to abort the `Update` when inspecting the state parameter that is passed in to
the `modify` function by throwing an exception. That happens before the update is performed and
a @apidoc[Replicator.ModifyFailure](akka.cluster.ddata.Replicator.ModifyFailure) is sent back as reply.

In the `Update` message you can pass an optional request context, which the @apidoc[akka.cluster.ddata.Replicator]
does not care about, but is included in the reply messages. This is a convenient
way to pass contextual information (e.g. original sender) without having to use `ask`
or maintain local correlation data structures.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #update-request-context }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #update-request-context }

<a id="replicator-get"></a>
### Get

For the full documentation of this feature and for new projects see @ref:[Distributed Data - Get](typed/distributed-data.md#get).

To retrieve the current value of a data you send @apidoc[Replicator.Get](akka.cluster.ddata.Replicator.Get) message to the
`Replicator`. You supply a consistency level which has the following meaning:

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #get }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #get }

As reply of the `Get` a @apidoc[Replicator.GetSuccess](akka.cluster.ddata.Replicator.GetSuccess) is sent to the sender of the
`Get` if the value was successfully retrieved according to the supplied @ref:[read consistency level](typed/distributed-data.md#read-consistency) within the supplied timeout. Otherwise a @apidoc[Replicator.GetFailure](akka.cluster.ddata.Replicator.GetFailure) is sent.
If the key does not exist the reply will be @apidoc[Replicator.NotFound](akka.cluster.ddata.Replicator.NotFound).

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #get-response1 }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #get-response1 }


Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #get-response2 }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #get-response2 }

In the @apidoc[akka.cluster.ddata.Replicator.Get] message you can pass an optional request context in the same way as for the
@apidoc[akka.cluster.ddata.Replicator.Update] message, described above. For example the original sender can be passed and replied
to after receiving and transforming @apidoc[akka.cluster.ddata.Replicator.GetSuccess].

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #get-request-context }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #get-request-context }

### Subscribe

For the full documentation of this feature and for new projects see @ref:[Distributed Data - Subscribe](typed/distributed-data.md#subscribe).

You may also register interest in change notifications by sending @apidoc[Replicator.Subscribe](akka.cluster.ddata.Replicator.Subscribe)
message to the `Replicator`. It will send @apidoc[Replicator.Changed](akka.cluster.ddata.Replicator.Changed) messages to the registered
subscriber when the data for the subscribed key is updated. Subscribers will be notified
periodically with the configured `notify-subscribers-interval`, and it is also possible to
send an explicit `Replicator.FlushChanges` message to the `Replicator` to notify the subscribers
immediately.

The subscriber is automatically removed if the subscriber is terminated. A subscriber can
also be deregistered with the @apidoc[Replicator.Unsubscribe](akka.cluster.ddata.Replicator.Unsubscribe) message.

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #subscribe }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #subscribe }

### Consistency

For the full documentation of this feature and for new projects see @ref:[Distributed Data Consistency](typed/distributed-data.md#consistency).
 
Here is an example of using @apidoc[akka.cluster.ddata.Replicator.WriteMajority] and @apidoc[akka.cluster.ddata.Replicator.ReadMajority]:

Scala
: @@snip [ShoppingCart.scala](/akka-docs/src/test/scala/docs/ddata/ShoppingCart.scala) { #read-write-majority }

Java
: @@snip [ShoppingCart.java](/akka-docs/src/test/java/jdocs/ddata/ShoppingCart.java) { #read-write-majority }


Scala
: @@snip [ShoppingCart.scala](/akka-docs/src/test/scala/docs/ddata/ShoppingCart.scala) { #get-cart }

Java
: @@snip [ShoppingCart.java](/akka-docs/src/test/java/jdocs/ddata/ShoppingCart.java) { #get-cart }


Scala
: @@snip [ShoppingCart.scala](/akka-docs/src/test/scala/docs/ddata/ShoppingCart.scala) { #add-item }

Java
: @@snip [ShoppingCart.java](/akka-docs/src/test/java/jdocs/ddata/ShoppingCart.java) { #add-item }

In some rare cases, when performing an @apidoc[akka.cluster.ddata.Replicator.Update] it is needed to first try to fetch latest data from
other nodes. That can be done by first sending a @apidoc[akka.cluster.ddata.Replicator.Get] with @apidoc[akka.cluster.ddata.Replicator.ReadMajority] and then continue with
the @apidoc[akka.cluster.ddata.Replicator.Update] when the @apidoc[akka.cluster.ddata.Replicator.GetSuccess], @apidoc[akka.cluster.ddata.Replicator.GetFailure] or @apidoc[akka.cluster.ddata.Replicator.NotFound] reply is received. This might be
needed when you need to base a decision on latest information or when removing entries from an @apidoc[akka.cluster.ddata.ORSet]
or @apidoc[akka.cluster.ddata.ORMap]. If an entry is added to an `ORSet` or `ORMap` from one node and removed from another
node the entry will only be removed if the added entry is visible on the node where the removal is
performed (hence the name observed-removed set).

The following example illustrates how to do that:

Scala
: @@snip [ShoppingCart.scala](/akka-docs/src/test/scala/docs/ddata/ShoppingCart.scala) { #remove-item }

Java
: @@snip [ShoppingCart.java](/akka-docs/src/test/java/jdocs/ddata/ShoppingCart.java) { #remove-item }

@@@ warning

*Caveat:* Even if you use `WriteMajority` and `ReadMajority` there is small risk that you may
read stale data if the cluster membership has changed between the `Update` and the `Get`.
For example, in cluster of 5 nodes when you `Update` and that change is written to 3 nodes:
n1, n2, n3. Then 2 more nodes are added and a `Get` request is reading from 4 nodes, which
happens to be n4, n5, n6, n7, i.e. the value on n1, n2, n3 is not seen in the response of the
`Get` request.

@@@

### Delete

For the full documentation of this feature and for new projects see @ref:[Distributed Data - Delete](typed/distributed-data.md#delete).

Scala
: @@snip [DistributedDataDocSpec.scala](/akka-docs/src/test/scala/docs/ddata/DistributedDataDocSpec.scala) { #delete }

Java
: @@snip [DistributedDataDocTest.java](/akka-docs/src/test/java/jdocs/ddata/DistributedDataDocTest.java) { #delete }

@@@ warning

As deleted keys continue to be included in the stored data on each node as well as in gossip
messages, a continuous series of updates and deletes of top-level entities will result in
growing memory usage until an ActorSystem runs out of memory. To use Akka Distributed Data
where frequent adds and removes are required, you should use a fixed number of top-level data
types that support both updates and removals, for example @apidoc[akka.cluster.ddata.ORMap] or @apidoc[akka.cluster.ddata.ORSet].

@@@

## Replicated data types

Akka contains a set of useful replicated data types and it is fully possible to implement custom replicated data types.
For the full documentation of this feature and for new projects see @ref:[Distributed Data Replicated data types](typed/distributed-data.md#replicated-data-types).

### Delta-CRDT
 
For the full documentation of this feature and for new projects see @ref:[Distributed Data Delta CRDT](typed/distributed-data.md#delta-crdt).

### Custom Data Type

You can implement your own data types. 
For the full documentation of this feature and for new projects see @ref:[Distributed Data custom data type](typed/distributed-data.md#custom-data-type).

<a id="ddata-durable"></a>
## Durable Storage

For the full documentation of this feature and for new projects see @ref:[Durable Storage](typed/distributed-data.md#durable-storage).

## Limitations

For the full documentation of this feature and for new projects see @ref:[Limitations](typed/distributed-data.md#limitations).

## Learn More about CRDTs

 * [Strong Eventual Consistency and Conflict-free Replicated Data Types (video)](https://www.youtube.com/watch?v=oyUHd894w18&amp;feature=youtu.be)
talk by Mark Shapiro
 * [A comprehensive study of Convergent and Commutative Replicated Data Types](https://hal.inria.fr/file/index/docid/555588/filename/techreport.pdf)
paper by Mark Shapiro et. al.

## Configuration

The @apidoc[akka.cluster.ddata.DistributedData] extension can be configured with the following properties:

@@snip [reference.conf](/akka-distributed-data/src/main/resources/reference.conf) { #distributed-data }
