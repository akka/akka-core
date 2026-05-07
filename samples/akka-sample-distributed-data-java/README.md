This tutorial contains 5 samples illustrating how to use [Akka Distributed Data](https://doc.akka.io/libraries/akka-core/current/typed/distributed-data.html).

- Low Latency Voting Service
- Highly Available Shopping Cart
- Distributed Service Registry
- Replicated Cache
- Replicated Metrics

To try this example locally, download the sources files with [akka-sample-distributed-data-java.zip](https://doc.akka.io/libraries/akka-core/current//attachments/akka-sample-distributed-data-java.zip).


**Akka Distributed Data** is useful when you need to share data between nodes in an Akka Cluster. The data is accessed with an actor providing a key-value store like API. The keys are unique identifiers with type information of the data values. The values are _Conflict Free Replicated Data Types_ (CRDTs).

All data entries are spread to all nodes, or nodes with a certain role, in the cluster via direct replication and gossip based dissemination. You have fine grained control of the consistency level for reads and writes.

The nature CRDTs makes it possible to perform updates from any node without coordination. Concurrent updates from different nodes will automatically be resolved by the monotonic merge function, which all data types must provide. The state changes always converge. Several useful data types for counters, sets, maps and registers are provided and you can also implement your own custom data types.

It is eventually consistent and geared toward providing high read and write availability (partition tolerance), with low latency. Note that in an eventually consistent system a read may return an out-of-date value.

Note that there are some [Limitations](https://doc.akka.io/libraries/akka-core/current//typed/distributed-data.html#limitations) that you should be aware of. For example, Akka Distributed Data is not intended for _Big Data_.

## Low Latency Voting Service

Distributed Data is great for low latency services, since you can update or get data from the local replica without immediate communication with other nodes.

Open [VotingService.java](src/main/java/sample/distributeddata/VotingService.java).

`VotingService` is an actor for low latency counting of votes on several cluster nodes and aggregation of the grand total number of votes. The actor is started on each cluster node. First it expects an `OPEN` message on one or several nodes. After that the counting can begin. The open signal is immediately replicated to all nodes with a boolean [Flag](https://doc.akka.io/libraries/akka-core/current//typed/distributed-data.html#limitations). Note `writeAll`.

    Update<Flag> update = new Update<>(openedKey, Flag.create(), writeAll, curr -> curr.switchOn());

The actor is subscribing to changes of the `OpenedKey` and other instances of this actor, also on other nodes, will be notified when the flag is changed.

    replicator.tell(new Subscribe<>(openedKey, self()), ActorRef.noSender());

    .match(Changed.class, c -> c.key().equals(openedKey), c -> receiveOpenedChanged((Changed<Flag>) c))

The counters are kept in a [PNCounterMap](https://doc.akka.io/libraries/akka-core/current//typed/distributed-data.html#counters) and updated with:

    Update<PNCounterMap> update = new Update<>(countersKey, PNCounterMap.create(), Replicator.writeLocal(),
            curr -> curr.increment(node, vote.participant, 1));
     replicator.tell(update, self());

Incrementing the counter is very fast, since it only involves communication with the local `Replicator` actor. Note `writeLocal`. Those updates are also spread to other nodes, but that is performed in the background.

The total number of votes is retrieved with:

    Optional<Object> ctx = Optional.of(sender());
    replicator.tell(new Replicator.Get<PNCounterMap>(countersKey, readAll, ctx), self());

    .match(GetSuccess.class, g -> g.key().equals(countersKey),
       g -> receiveGetSuccess(open, (GetSuccess<PNCounterMap>) g))

    private void receiveGetSuccess(boolean open, GetSuccess<PNCounterMap> g) {
      Map<String, BigInteger> result = g.dataValue().getEntries();
      ActorRef replyTo = (ActorRef) g.getRequest().get();
      replyTo.tell(new Votes(result, open), self());
    }

The multi-node test for the `VotingService` can be found in [VotingServiceSpec.scala](src/multi-jvm/scala/sample/distributeddata/VotingServiceSpec.scala).

Read the [Using the Replicator](https://doc.akka.io/libraries/akka-core/current//typed/distributed-data.html#using-the-replicator) documentation for more details of how to use `Get`, `Update`, and `Subscribe`.

## Highly Available Shopping Cart

Distributed Data is great for highly available services, since it is possible to perform updates to the local node (or currently available nodes) during a network partition.

Open [ShoppingCart.java](src/main/java/sample/distributeddata/ShoppingCart.java).

`ShoppingCart` is an actor that holds the selected items to buy for a user. The actor instance for a specific user may be started where ever needed in the cluster, i.e. several instances may be started on different nodes and used at the same time.

Each product in the cart is represented by a `LineItem` and all items in the cart is collected in a [LWWMap](https://doc.akka.io/libraries/akka-core/current//typed/distributed-data.html#maps).

The actor handles the commands `GET_CART`, `AddItem` and `RemoveItem`. To get the latest updates in case the same shopping cart is used from several nodes it is using consistency level of `readMajority` and `writeMajority`, but that is only done to reduce the risk of seeing old data. If such reads and writes cannot be completed due to a network partition it falls back to reading/writing from the local replica (see `GetFailure`). Local reads and writes will always be successful and when the network partition heals the updated shopping carts will be be disseminated by the [gossip protocol](https://en.wikipedia.org/wiki/Gossip_protocol) and the `LWWMap` CRDTs are merged, i.e. it is a highly available shopping cart.

The multi-node test for the `ShoppingCart` can be found in [ShoppingCartSpec.scala](src/multi-jvm/scala/sample/distributeddata/ShoppingCartSpec.scala).

Read the [Consistency](https://doc.akka.io/libraries/akka-core/current//typed/distributed-data.html#consistency) section in the documentation to understand the consistency considerations.

## Replicated Cache

This example illustrates a simple key-value cache.

Open [ReplicatedCache.scala](src/main/java/sample/distributeddata/ReplicatedCache.java).

`ReplicatedCache` is an actor that is started on each node in the cluster. It supports three commands: `PutInCache`, `GetFromCache` and `Evict`.

It is splitting up the key space in 100 top level keys, each with a `LWWMap`. When a data entry is changed the full state of that entry is replicated to other nodes, i.e. when you update a map the whole map is replicated. Therefore, instead of using one ORMap with 1000 elements it is more efficient to split that up in 100 top level ORMap entries with 10 elements each. Top level entries are replicated individually, which has the trade-off that different entries may not be replicated at the same time and you may see inconsistencies between related entries. Separate top level entries cannot be updated atomically together.

The multi-node test for the `ReplicatedCache` can be found in [ReplicatedCacheSpec.scala](src/multi-jvm/scala/sample/distributeddata/ReplicatedCacheSpec.scala).

## Replicated Metrics

This example illustrates to spread metrics data to all nodes in an Akka cluster.

Open [ReplicatedMetrics.java](src/main/java/sample/distributeddata/ReplicatedMetrics.java).

`ReplicatedMetrics` is an actor that is started on each node in the cluster. Periodically it collects some metrics, in this case used and max heap size. Each metrics type is stored in a `LWWMap` where the key in the map is the address of the node. The values are disseminated to other nodes with the gossip protocol.

The multi-node test for the `ReplicatedCache` can be found in [ReplicatedMetricsSpec.scala](src/multi-jvm/scala/sample/distributeddata/ReplicatedMetricsSpec.scala).

Note that there are some [Limitations](https://doc.akka.io/libraries/akka-core/current//typed/distributed-data.html#limitations) that you should be aware of. For example, Akka Distributed Data is not intended for _Big Data_.

---

The Akka family of projects is managed by teams at Lightbend with help from the community.

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).
