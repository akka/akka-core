/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.NotUsed
import akka.stream.{ KillSwitches, UniqueKillSwitch }
import akka.stream.scaladsl._
import akka.testkit.AkkaSpec
import docs.CompileOnlySpec

import scala.concurrent.duration._

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
class HubsDocSpec extends AkkaSpec with CompileOnlySpec {

  "Hubs" must {

    "demonstrate creating a dynamic merge" in {
      def println(s: String) = testActor ! s

      //#merge-hub
      // A simple consumer that will print to the console for now
      val consumer = Sink.foreach(println)

      // Attach a MergeHub Source to the consumer. This will materialize to a
      // corresponding Sink.
      val runnableGraph: RunnableGraph[Sink[String, NotUsed]] =
        MergeHub.source[String](perProducerBufferSize = 16).to(consumer)

      // By running/materializing the consumer we get back a Sink, and hence
      // now have access to feed elements into it. This Sink can be materialized
      // any number of times, and every element that enters the Sink will
      // be consumed by our consumer.
      val toConsumer: Sink[String, NotUsed] = runnableGraph.run()

      // Feeding two independent sources into the hub.
      Source.single("Hello!").runWith(toConsumer)
      Source.single("Hub!").runWith(toConsumer)
      //#merge-hub

      expectMsgAllOf("Hello!", "Hub!")
    }

    "demonstrate creating a dynamic broadcast" in compileOnlySpec {
      //#broadcast-hub
      // A simple producer that publishes a new "message" every second
      val producer = Source.tick(1.second, 1.second, "New message")

      // Attach a BroadcastHub Sink to the producer. This will materialize to a
      // corresponding Source.
      // (We need to use toMat and Keep.right since by default the materialized
      // value to the left is used)
      val runnableGraph: RunnableGraph[Source[String, NotUsed]] =
        producer.toMat(BroadcastHub.sink(bufferSize = 256))(Keep.right)

      // By running/materializing the producer, we get back a Source, which
      // gives us access to the elements published by the producer.
      val fromProducer: Source[String, NotUsed] = runnableGraph.run()

      // Print out messages from the producer in two independent consumers
      fromProducer.runForeach(msg => println("consumer1: " + msg))
      fromProducer.runForeach(msg => println("consumer2: " + msg))
      //#broadcast-hub
    }

    "demonstrate combination" in {
      def println(s: String) = testActor ! s

      //#pub-sub-1
      // Obtain a Sink and Source which will publish and receive from the "bus" respectively.
      val (sink, source) =
        MergeHub.source[String](perProducerBufferSize = 16).toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both).run()
      //#pub-sub-1

      //#pub-sub-2
      // Ensure that the Broadcast output is dropped if there are no listening parties.
      // If this dropping Sink is not attached, then the broadcast hub will not drop any
      // elements itself when there are no subscribers, backpressuring the producer instead.
      source.runWith(Sink.ignore)
      //#pub-sub-2

      //#pub-sub-3
      // We create now a Flow that represents a publish-subscribe channel using the above
      // started stream as its "topic". We add two more features, external cancellation of
      // the registration and automatic cleanup for very slow subscribers.
      val busFlow: Flow[String, String, UniqueKillSwitch] =
        Flow
          .fromSinkAndSource(sink, source)
          .joinMat(KillSwitches.singleBidi[String, String])(Keep.right)
          .backpressureTimeout(3.seconds)
      //#pub-sub-3

      //#pub-sub-4
      val switch: UniqueKillSwitch =
        Source.repeat("Hello world!").viaMat(busFlow)(Keep.right).to(Sink.foreach(println)).run()

      // Shut down externally
      switch.shutdown()
      //#pub-sub-4
    }

    "demonstrate creating a dynamic partition hub" in compileOnlySpec {
      //#partition-hub
      // A simple producer that publishes a new "message-" every second
      val producer = Source.tick(1.second, 1.second, "message").zipWith(Source(1 to 100))((a, b) => s"$a-$b")

      // Attach a PartitionHub Sink to the producer. This will materialize to a
      // corresponding Source.
      // (We need to use toMat and Keep.right since by default the materialized
      // value to the left is used)
      val runnableGraph: RunnableGraph[Source[String, NotUsed]] =
        producer.toMat(
          PartitionHub.sink(
            (size, elem) => math.abs(elem.hashCode % size),
            startAfterNrOfConsumers = 2,
            bufferSize = 256))(Keep.right)

      // By running/materializing the producer, we get back a Source, which
      // gives us access to the elements published by the producer.
      val fromProducer: Source[String, NotUsed] = runnableGraph.run()

      // Print out messages from the producer in two independent consumers
      fromProducer.runForeach(msg => println("consumer1: " + msg))
      fromProducer.runForeach(msg => println("consumer2: " + msg))
      //#partition-hub
    }

    "demonstrate creating a dynamic stateful partition hub" in compileOnlySpec {
      //#partition-hub-stateful
      // A simple producer that publishes a new "message-" every second
      val producer = Source.tick(1.second, 1.second, "message").zipWith(Source(1 to 100))((a, b) => s"$a-$b")

      // New instance of the partitioner function and its state is created
      // for each materialization of the PartitionHub.
      def roundRobin(): (PartitionHub.ConsumerInfo, String) => Long = {
        var i = -1L

        (info, elem) => {
          i += 1
          info.consumerIdByIdx((i % info.size).toInt)
        }
      }

      // Attach a PartitionHub Sink to the producer. This will materialize to a
      // corresponding Source.
      // (We need to use toMat and Keep.right since by default the materialized
      // value to the left is used)
      val runnableGraph: RunnableGraph[Source[String, NotUsed]] =
        producer.toMat(PartitionHub.statefulSink(() => roundRobin(), startAfterNrOfConsumers = 2, bufferSize = 256))(
          Keep.right)

      // By running/materializing the producer, we get back a Source, which
      // gives us access to the elements published by the producer.
      val fromProducer: Source[String, NotUsed] = runnableGraph.run()

      // Print out messages from the producer in two independent consumers
      fromProducer.runForeach(msg => println("consumer1: " + msg))
      fromProducer.runForeach(msg => println("consumer2: " + msg))
      //#partition-hub-stateful
    }

    "demonstrate creating a dynamic partition hub routing to fastest consumer" in compileOnlySpec {
      //#partition-hub-fastest
      val producer = Source(0 until 100)

      // ConsumerInfo.queueSize is the approximate number of buffered elements for a consumer.
      // Note that this is a moving target since the elements are consumed concurrently.
      val runnableGraph: RunnableGraph[Source[Int, NotUsed]] =
        producer.toMat(
          PartitionHub.statefulSink(
            () => (info, elem) => info.consumerIds.minBy(id => info.queueSize(id)),
            startAfterNrOfConsumers = 2,
            bufferSize = 16))(Keep.right)

      val fromProducer: Source[Int, NotUsed] = runnableGraph.run()

      fromProducer.runForeach(msg => println("consumer1: " + msg))
      fromProducer.throttle(10, 100.millis).runForeach(msg => println("consumer2: " + msg))
      //#partition-hub-fastest
    }

  }

}
