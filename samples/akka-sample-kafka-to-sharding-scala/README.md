# Aligning Kafka Partitions with Akka Cluster Sharding 

It is common to consume a Kafka topic and forward the messages to sharded actors. 

The Kafka consumer can be started on each node with the same group id
and then the messages forwarded to sharding via an ask. It is important to use  ask
rather than tell to enable backpressure from the sharded actor to the Kafka consumer. 

Using the default shard allocation strategy there is no relation between the Kafka partitions
allocated to a consumer and the location of the shards meaning that most messages will 
have one network hop.

If all of the messages for the same sharded entity are in the same Kafka partition then
this can be improved on with the external shard allocation strategy.
For this to be true the producer partitioning must align with the shard extraction 
in cluster sharding. 

Imagine a scenario that processes all events for users with following constraints:
 * The key of the kafka message is the user id which is in turn the entity id in sharding
 * All messages for the same user id end up in the same partition
 
Then we can enforce that the kafka partition == the akka cluster shard id and use the external 
sharding allocation strategy to move shards to the node that is consuming that partition, resulting
in no cross node traffic.

Read the following documentation to learn more about [Akka Cluster External Shard Allocation](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html#external-shard-allocation) 
and its support for Kafka in [Alpakka Kafka Cluster Sharding](https://doc.akka.io/librarires/alpakka-kafka/current/cluster-sharding.html).

# Running the sample 

The sample is made up of three applications:
* `producer` A Kafka producer, that produces events about users 
* `processor` An Akka Cluster Sharding application that reads the Kafka topic and forwards the messages to a sharded
              entity that represents a user and a gRPC front end for accessing the sharded actors state
* `client` A gRPC client for interacting with the cluster
* `kafka` A local Kafka server
              
The sample demonstrates how the external shard allocation strategy can be used so messages are processed locally.

The sample depends on a Kafka broker running locally on port `9092` with a topic with 128 partitions called `user-events`. 
[Kafka can be run in Docker](https://github.com/wurstmeister/kafka-docker) or run locally using the optional `kafka` project.

* Run the local Kafka server. This project will also create the `user-events` topic.

```
sbt "kafka / run"
```

In the Kafka server window you'll see the following when the server is ready:

```
12:06:59.711 INFO  [run-main-0          ] s.s.embeddedkafka.KafkaBroker$        Kafka running: localhost:9092
12:06:59.711 INFO  [run-main-0          ] s.s.embeddedkafka.KafkaBroker$        Topic 'user-events' with 128 partitions created
```

If you want to use a different Kafka cluster then update the `applications.conf`s in each project to point to your 
Kafka broker if not running on `localhost:9092`.


* _(Optional)_ If you do not run the local Kafka server then you must create a topic with 128 partitions, or update 
  application.conf with the desired number of partitions e.g. a command from your Kafka installation:
  
```
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 128 --topic user-events
```
  
* Start a single processor, this will consume the messages from the topic and distribute them to sharding,
  three arguments are required, the akka remoting port, the akka management port, and the gRPC port for the front end. 
* If you run on different ports the first two akka remoting ports should be 2551/2552 as they are configured as seeds.
* As there is a single consumer, all partitions will initially be assigned to this node.

```
sbt "processor / run 2551 8551 8081"
```

The processor starts a KafkaConsumer, as it is the only consumer in the group it will be assigned every single Kafka partition
and shards for each partition will be assigned to the current node. You will see logs like:

```
[info] [2020-01-16 09:48:51,040] [INFO] [akka://KafkaToSharding/user/kafka-event-processor/rebalancerRef] - Partition [1] assigned to current node. Updating shard allocation
[info] [2020-01-16 09:48:51,040] [INFO] [akka://KafkaToSharding/user/kafka-event-processor/rebalancerRef] - Partition [25] assigned to current node. Updating shard allocation
[info] [2020-01-16 09:48:51,043] [INFO] [akka://KafkaToSharding/user/kafka-event-processor/rebalancerRef] - Partition [116] assigned to current node. Updating shard allocation
```

If there are existing messages on the topic they will all be processed locally as there is a single node.

Next we start the Kafka producer to see some messages flowing from Kafka to sharding.

```
sbt "producer / run"
```

In the producer window you'll see:

```
[INFO] [01/16/2020 09:51:38.639] [UserEventProducer(akka://UserEventProducer)] Sending message to user 29
[INFO] [01/16/2020 09:51:39.660] [UserEventProducer(akka://UserEventProducer)] Sending message to user 60
[INFO] [01/16/2020 09:51:40.680] [UserEventProducer(akka://UserEventProducer)] Sending message to user 75

```

In the single processor node the messages will start flowing:

```
[info] [2020-01-16 09:51:38,672] [INFO] [sample.sharding.kafka.UserEventsKafkaProcessor$] [KafkaToSharding-akka.actor.default-dispatcher-26] [akka://KafkaToSharding/user/kafka-event-processor] - entityId->partition 29->45
[info] [2020-01-16 09:51:38,672] [INFO] [sample.sharding.kafka.UserEventsKafkaProcessor$] [KafkaToSharding-akka.actor.default-dispatcher-26] [akka://KafkaToSharding/user/kafka-event-processor] - Forwarding message for entity 29 to cluster sharding
[info] [2020-01-16 09:51:38,673] [INFO] [sample.sharding.kafka.UserEvents$] [KafkaToSharding-akka.actor.default-dispatcher-26] [akka://KafkaToSharding/system/sharding/user-processing/75/29] - user 29 purchase cat t-shirt, quantity 0, price 8874
[info] [2020-01-16 09:51:39,702] [INFO] [sample.sharding.kafka.UserEventsKafkaProcessor$] [KafkaToSharding-akka.actor.default-dispatcher-17] [akka://KafkaToSharding/user/kafka-event-processor] - entityId->partition 60->111
[info] [2020-01-16 09:51:39,703] [INFO] [sample.sharding.kafka.UserEventsKafkaProcessor$] [KafkaToSharding-akka.actor.default-dispatcher-17] [akka://KafkaToSharding/user/kafka-event-processor] - Forwarding message for entity 60 to cluster sharding
[info] [2020-01-16 09:51:39,706] [INFO] [sample.sharding.kafka.UserEvents$] [KafkaToSharding-akka.actor.default-dispatcher-17] [akka://KafkaToSharding/system/sharding/user-processing/2/60] - user 60 purchase cat t-shirt, quantity 2, price 9375
[info] [2020-01-16 09:51:40,732] [INFO] [sample.sharding.kafka.UserEventsKafkaProcessor$] [KafkaToSharding-akka.actor.default-dispatcher-17] [akka://KafkaToSharding/user/kafka-event-processor] - entityId->partition 75->1
[info] [2020-01-16 09:51:40,732] [INFO] [sample.sharding.kafka.UserEventsKafkaProcessor$] [KafkaToSharding-akka.actor.default-dispatcher-17] [akka://KafkaToSharding/user/kafka-event-processor] - Forwarding message for entity 75 to cluster sharding
```

The first log line is just after the message has been taken from Kafka.
The second log is from the sharded entity. The goal is to have these
always on the same node as the external shard allocation strategy will move the shard to where ever the
Kafka partition is being consumed.

As there is only one node we get 100% locality, each forwarded message is processed on the same node

Now let's see that remain true once we add more nodes to the Akka Cluster, add another with different ports:

```
sbt "processor / run 2552 8552 8082"
```

When this starts up we'll see Kafka assign partitions to the new node (it is in the same consumer group):

```
Partition [29] assigned to current node. Updating shard allocation
```

On one of the nodes, where the ShardCoordinator runs, we'll see the rebalance happening:

```
[info] [2020-01-16 09:59:39,923] [INFO] [akka://KafkaToSharding@127.0.0.1:2551/system/sharding/user-processingCoordinator/singleton/coordinator] - Starting rebalance for shards [45,33,16,2,3,15,11,6,36]. Current shards rebalancing: []
```

Both nodes now have roughly 64 shards / partitions, all co-located with the Kafka Consuemer.
You can verify this by the logs showing that when a message is received by the Kafka Consumer when it is forwarded to 
cluster sharding the entity logs receiving the event on the same node. 

```
[info] [2020-01-17 08:27:58,199] [INFO] [akka://KafkaToSharding/user/kafka-event-processor] - Forwarding message for entity 29 to cluster sharding
[info] [2020-01-17 08:27:58,204] [INFO] [akka://KafkaToSharding/system/sharding/user-processing/45/29] - user 29 purchase cat t-shirt, quantity 1, price 2093
[info] [2020-01-17 08:28:08,218] [INFO] [akka://KafkaToSharding/user/kafka-event-processor] - Forwarding message for entity 56 to cluster sharding
[info] [2020-01-17 08:28:08,218] [INFO] [akka://KafkaToSharding/system/sharding/user-processing/6/56] - user 56 purchase akka t-shirt, quantity 3, price 8576
[info] [2020-01-17 08:28:28,288] [INFO] [akka://KafkaToSharding/user/kafka-event-processor] - Forwarding message for entity 44 to cluster sharding
[info] [2020-01-17 08:28:28,296] [INFO] [akka://KafkaToSharding/system/sharding/user-processing/59/44] - user 44 purchase cat t-shirt, quantity 3, price 9716
```

Each forwarding messaging is followed by log for the same entity on the current node.

Using Akka management we can see the shard allocations and the number of entities per shard (uses `curl` and `jq`):

```
# Node 1:
curl -v localhost:8551/cluster/shards/user-processing | jq

# Node 2:
curl -v localhost:8552/cluster/shards/user-processing | jq
```

We can count the number of shards on each:

```
# Node 1:
curl -s localhost:8551/cluster/shards/user-processing | jq -r "." | grep shardId | wc -l
# Node 2:
curl -s localhost:8552/cluster/shards/user-processing | jq -r "." | grep shardId | wc -l
```

The number of shards will depend on which entities have received messages.

We now have a 2 node Akka Cluster with a Kafka Consumer running on each where the kafka partitions align 
with Cluster shards.

A use case for sending the processing to sharding is it allows each entity to be queried from any where in the cluster
e.g. from a HTTP or gRPC front end.

The sample includes a gRPC front end that gets the running total of number of purchases and total money spent
by each customer. Requests can come via gRPC on any node for any entity but sharding will route them to
the correct node even if that moves due to a kafka rebalance.

A gRPC client is included which can be started with...

```
sbt "client / run"
```

It assumes there is one of the nodes running its front end port on 8081. 
You can enter the user id to get their stats: The users are `0-199`, try entering users
that have shown up in the logs of the other processes.

```
7
User 7 has made 2 for a total of 3096p
Enter user id or :q to quit
3
User 3 has made 1 for a total of 12060p
Enter user id or :q to quit
4
User 4 has made 1 for a total of 7876p
Enter user id or :q to quit
5
User 5 has made 0 for a total of 0p
Enter user id or :q to quit
1
User 1 has made 0 for a total of 0p
Enter user id or :q to quit
```

We've now demonstrated two things:

* Keeping the processing local, where ever the Kafka partition is consumed the shard will be moved to that location
* The state for each entity is globally accessible from all nodes 

---

The Akka family of projects is managed by teams at Lightbend with help from the community.

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).
