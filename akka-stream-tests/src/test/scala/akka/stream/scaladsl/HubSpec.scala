/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._

import akka.Done
import akka.stream.KillSwitches
import akka.stream.ThrottleMode
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.EventFilter

class HubSpec extends StreamSpec {
  implicit val ec: ExecutionContext = system.dispatcher

  "MergeHub" must {

    "work in the happy case" in {
      val (sink, result) = MergeHub.source[Int](16).take(20).toMat(Sink.seq)(Keep.both).run()
      Source(1 to 10).runWith(sink)
      Source(11 to 20).runWith(sink)

      result.futureValue.sorted should ===(1 to 20)
    }

    "notify new producers if consumer cancels before first producer" in {
      val sink = Sink.cancelled[Int].runWith(MergeHub.source[Int](16))
      val upstream = TestPublisher.probe[Int]()

      Source.fromPublisher(upstream).runWith(sink)

      upstream.expectCancellation()
    }

    "notify existing producers if consumer cancels after a few elements" in {
      val (sink, result) = MergeHub.source[Int](16).take(5).toMat(Sink.seq)(Keep.both).run()
      val upstream = TestPublisher.probe[Int]()

      Source.fromPublisher(upstream).runWith(sink)
      for (i <- 1 to 5) upstream.sendNext(i)

      upstream.expectCancellation()
      result.futureValue.sorted should ===(1 to 5)
    }

    "notify new producers if consumer cancels after a few elements" in {
      val (sink, result) = MergeHub.source[Int](16).take(5).toMat(Sink.seq)(Keep.both).run()
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[Int]()

      Source.fromPublisher(upstream1).runWith(sink)
      for (i <- 1 to 5) upstream1.sendNext(i)

      upstream1.expectCancellation()
      result.futureValue.sorted should ===(1 to 5)

      Source.fromPublisher(upstream2).runWith(sink)

      upstream2.expectCancellation()
    }

    "respect buffer size" in {
      val downstream = TestSubscriber.manualProbe[Int]()
      val sink = Sink.fromSubscriber(downstream).runWith(MergeHub.source[Int](3))

      Source(1 to 10)
        .map { i =>
          testActor ! i; i
        }
        .runWith(sink)

      val sub = downstream.expectSubscription()
      sub.request(1)

      // Demand starts from 3
      expectMsg(1)
      expectMsg(2)
      expectMsg(3)
      expectNoMessage(100.millis)

      // One element consumed (it was requested), demand 0 remains at producer
      downstream.expectNext(1)

      // Requesting next element, results in next element to be consumed.
      sub.request(1)
      downstream.expectNext(2)

      // Two elements have been consumed, so threshold of 2 is reached, additional 2 demand is dispatched.
      // There is 2 demand at the producer now

      expectMsg(4)
      expectMsg(5)
      expectNoMessage(100.millis)

      // Two additional elements have been sent:
      //  - 3, 4, 5 are pending
      //  - demand is 0 at the producer
      //  - next demand batch is after two elements have been consumed again

      // Requesting next gives the next element
      // Demand is not yet refreshed for the producer as there is one more element until threshold is met
      sub.request(1)
      downstream.expectNext(3)

      expectNoMessage(100.millis)

      sub.request(1)
      downstream.expectNext(4)
      expectMsg(6)
      expectMsg(7)

      sub.cancel()
    }

    "work with long streams" in {
      val (sink, result) = MergeHub.source[Int](16).take(20000).toMat(Sink.seq)(Keep.both).run()
      Source(1 to 10000).runWith(sink)
      Source(10001 to 20000).runWith(sink)

      result.futureValue.sorted should ===(1 to 20000)
    }

    "work with long streams when buffer size is 1" in {
      val (sink, result) = MergeHub.source[Int](1).take(20000).toMat(Sink.seq)(Keep.both).run()
      Source(1 to 10000).runWith(sink)
      Source(10001 to 20000).runWith(sink)

      result.futureValue.sorted should ===(1 to 20000)
    }

    "work with long streams when consumer is slower" in {
      val (sink, result) =
        MergeHub
          .source[Int](16)
          .take(2000)
          .throttle(10, 1.millisecond, 200, ThrottleMode.shaping)
          .toMat(Sink.seq)(Keep.both)
          .run()

      Source(1 to 1000).runWith(sink)
      Source(1001 to 2000).runWith(sink)

      result.futureValue.sorted should ===(1 to 2000)
    }

    "work with long streams if one of the producers is slower" in {
      val (sink, result) =
        MergeHub.source[Int](16).take(2000).toMat(Sink.seq)(Keep.both).run()

      Source(1 to 1000).throttle(10, 1.millisecond, 100, ThrottleMode.shaping).runWith(sink)
      Source(1001 to 2000).runWith(sink)

      result.futureValue.sorted should ===(1 to 2000)
    }

    "work with different producers separated over time" in {
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()
      val sink = MergeHub.source[Int](16).grouped(100).toMat(Sink.fromSubscriber(downstream))(Keep.left).run()

      Source(1 to 100).runWith(sink)
      downstream.requestNext() should ===(1 to 100)

      Source(101 to 200).runWith(sink)
      downstream.requestNext() should ===(101 to 200)

      downstream.cancel()
    }

    "keep working even if one of the producers fail" in {
      val (sink, result) = MergeHub.source[Int](16).take(10).toMat(Sink.seq)(Keep.both).run()
      EventFilter.error("Upstream producer failed with exception").intercept {
        Source.failed(TE("failing")).runWith(sink)
        Source(1 to 10).runWith(sink)
      }

      result.futureValue.sorted should ===(1 to 10)

    }

    "complete after draining control is invoked and all connected producers complete" in {
      val downstream = TestSubscriber.probe[Int]()
      val (sink, draining) =
        MergeHub.sourceWithDraining[Int](16).toMat(Sink.fromSubscriber(downstream))(Keep.left).run()

      val probe1 = TestPublisher.probe[Int]()
      val probe2 = TestPublisher.probe[Int]()

      Source.fromPublisher(probe1).concat(Source(1 to 10)).runWith(sink)
      Source.fromPublisher(probe2).concat(Source(11 to 20)).runWith(sink)

      // Wait for the sources to be registered with the MergeHub before starting the draining process
      downstream.request(2)
      probe1.sendNext(-1)
      probe2.sendNext(-2)
      downstream.expectNextN(2) should contain theSameElementsAs (List(-1, -2))

      draining.drainAndComplete()

      // Give time to the MergeHub to register the draining request
      val testResult = Promise[Done]()
      system.scheduler.scheduleOnce(200.millis) {
        probe1.sendComplete()
        probe2.sendComplete()

        testResult.completeWith(Future {
          downstream.request(20)
          downstream.expectNextN(20).sorted should ===(1 to 20)

          downstream.request(1)
          downstream.expectComplete()
          Done
        })
      }
      testResult.future.futureValue
    }

    "immediately cancel new producers while draining" in {
      val downstream = TestSubscriber.probe[Int]()
      val (sink, draining) =
        MergeHub.sourceWithDraining[Int](16).toMat(Sink.fromSubscriber(downstream))(Keep.left).run()

      val probe1 = TestPublisher.probe[Int]()
      val probe2 = TestPublisher.probe[Int]()

      Source.fromPublisher(probe1).concat(Source(1 to 10)).runWith(sink)
      Source.fromPublisher(probe2).concat(Source(11 to 20)).runWith(sink)

      // Wait for the sources to be registered with the MergeHub before starting the draining process
      downstream.request(2)
      probe1.sendNext(-1)
      probe2.sendNext(-2)
      downstream.expectNextN(2) should contain theSameElementsAs (List(-1, -2))

      draining.drainAndComplete()

      // Give time to the MergeHub to register the draining request
      val testResult = Promise[Done]()
      system.scheduler.scheduleOnce(200.millis) {
        testResult.completeWith(Future {
          val upstream = TestPublisher.probe[Int]()
          Source.fromPublisher(upstream).runWith(sink)
          upstream.expectCancellation()

          probe1.sendComplete()
          probe2.sendComplete()

          downstream.request(20)
          downstream.expectNextN(20).sorted should ===(1 to 20)

          downstream.request(1)
          downstream.expectComplete()
          Done
        })
      }
      testResult.future.futureValue
    }

    "immediately complete if no producers are registered" in {
      val downstream = TestSubscriber.probe[Int]()
      val (_, draining) =
        MergeHub.sourceWithDraining[Int](16).toMat(Sink.fromSubscriber(downstream))(Keep.left).run()

      draining.drainAndComplete()
      downstream.request(1)
      downstream.expectComplete()
    }
  }

  "BroadcastHub" must {

    "work in the happy case" in {
      val source = Source(1 to 10).runWith(BroadcastHub.sink(8))
      source.runWith(Sink.seq).futureValue should ===(1 to 10)
    }

    "send the same elements to consumers attaching around the same time" in {
      val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 10)).toMat(BroadcastHub.sink(8))(Keep.both).run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "broadcast elements to downstream after at least one subscriber" in {
      val broadcast = Source(1 to 10).runWith(BroadcastHub.sink[Int](1, 256))
      val resultOne = broadcast.runWith(Sink.seq) // nothing happening yet

      Await.result(resultOne, 1.second) should be(1 to 10) // fails
    }

    "broadcast all elements to all consumers" in {
      val sourceQueue = Source.queue[Int](10) // used to block the source until we say so
      val (queue, broadcast) = sourceQueue.toMat(BroadcastHub.sink(2, 256))(Keep.both).run()
      val resultOne = broadcast.runWith(Sink.seq) // nothing happening yet
      for (i <- 1 to 5) {
        queue.offer(i)
      }
      val resultTwo = broadcast.runWith(Sink.seq)
      for (i <- 6 to 10) {
        queue.offer(i)
      }
      queue.complete() // only now is the source emptied

      Await.result(resultOne, 1.second) should be(1 to 10)
      Await.result(resultTwo, 1.second) should be(1 to 10)
    }

    "broadcast all elements to all consumers with hot upstream" in {
      val broadcast = Source(1 to 10).runWith(BroadcastHub.sink[Int](2, 256))
      val resultOne = broadcast.runWith(Sink.seq) // nothing happening yet
      val resultTwo = broadcast.runWith(Sink.seq)

      Await.result(resultOne, 1.second) should be(1 to 10)
      Await.result(resultTwo, 1.second) should be(1 to 10)
    }

    "broadcast all elements to all consumers with hot upstream even some subscriber unsubscribe" in {
      val broadcast = Source(1 to 10).runWith(BroadcastHub.sink[Int](2, 256))
      val sub = broadcast.runWith(TestSink.apply())
      sub.request(1)
      Thread.sleep(1000)
      sub.cancel()
      val resultOne = broadcast.runWith(Sink.seq) // nothing happening yet
      val resultTwo = broadcast.runWith(Sink.seq) // nothing happening yet

      Await.result(resultOne, 1.second) should be(1 to 10)
      Await.result(resultTwo, 1.second) should be(1 to 10)
    }

    "send the same prefix to consumers attaching around the same time if one cancels earlier" in {
      val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 20)).toMat(BroadcastHub.sink(8))(Keep.both).run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.take(10).runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 20)
      f2.futureValue should ===(1 to 10)
    }

    "ensure that subsequent consumers see subsequent elements without gap" in {
      val source = Source(1 to 20).runWith(BroadcastHub.sink(8))
      source.take(10).runWith(Sink.seq).futureValue should ===(1 to 10)
      source.take(10).runWith(Sink.seq).futureValue should ===(11 to 20)
    }

    "send the same elements to consumers of different speed attaching around the same time" in {
      val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 10)).toMat(BroadcastHub.sink(8))(Keep.both).run()

      val f1 = source.throttle(1, 10.millis, 3, ThrottleMode.shaping).runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "send the same elements to consumers of attaching around the same time if the producer is slow" in {
      val (firstElem, source) = Source
        .maybe[Int]
        .concat(Source(2 to 10))
        .throttle(1, 10.millis, 3, ThrottleMode.shaping)
        .toMat(BroadcastHub.sink(8))(Keep.both)
        .run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "ensure that from two different speed consumers the slower controls the rate" in {
      val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 20)).toMat(BroadcastHub.sink(1))(Keep.both).run()

      val f1 = source.throttle(1, 10.millis, 1, ThrottleMode.shaping).runWith(Sink.seq)
      // Second cannot be overwhelmed since the first one throttles the overall rate, and second allows a higher rate
      val f2 = source.throttle(10, 10.millis, 8, ThrottleMode.enforcing).runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 20)
      f2.futureValue should ===(1 to 20)

    }

    "send the same elements to consumers attaching around the same time with a buffer size of one" in {
      val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 10)).toMat(BroadcastHub.sink(1))(Keep.both).run()

      val f1 = source.runWith(Sink.seq)
      val f2 = source.runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(1))
      f1.futureValue should ===(1 to 10)
      f2.futureValue should ===(1 to 10)
    }

    "be able to implement a keep-dropping-if-unsubscribed policy with a simple Sink.ignore" in {
      val killSwitch = KillSwitches.shared("test-switch")
      val source = Source.fromIterator(() => Iterator.from(0)).via(killSwitch.flow).runWith(BroadcastHub.sink(8))

      // Now the Hub "drops" elements until we attach a new consumer (Source.ignore consumes as fast as possible)
      source.runWith(Sink.ignore)

      // Now we attached a subscriber which will block the Sink.ignore to "take away" and drop elements anymore,
      // turning the BroadcastHub to a normal non-dropping mode
      val downstream = TestSubscriber.probe[Int]()
      source.runWith(Sink.fromSubscriber(downstream))

      downstream.request(1)
      val first = downstream.expectNext()

      for (i <- (first + 1) to (first + 10)) {
        downstream.request(1)
        downstream.expectNext(i)
      }

      downstream.cancel()

      killSwitch.shutdown()
    }

    "properly signal error to consumers" in {
      val upstream = TestPublisher.probe[Int]()
      val source = Source.fromPublisher(upstream).runWith(BroadcastHub.sink(8))

      val downstream1 = TestSubscriber.probe[Int]()
      val downstream2 = TestSubscriber.probe[Int]()
      source.runWith(Sink.fromSubscriber(downstream1))
      source.runWith(Sink.fromSubscriber(downstream2))

      downstream1.request(4)
      downstream2.request(8)

      // sending the first element is in a race with downstream subscribing
      // give a bit of time for the downstream to complete subscriptions
      Thread.sleep(100)

      (1 to 8).foreach(upstream.sendNext(_))

      downstream1.expectNext(1, 2, 3, 4)
      downstream2.expectNext(1, 2, 3, 4, 5, 6, 7, 8)

      downstream1.expectNoMessage(100.millis)
      downstream2.expectNoMessage(100.millis)

      upstream.sendError(TE("Failed"))

      downstream1.expectError(TE("Failed"))
      downstream2.expectError(TE("Failed"))
    }

    "properly signal completion to consumers arriving after producer finished" in {
      val source = Source.empty[Int].runWith(BroadcastHub.sink(8))
      // Wait enough so the Hub gets the completion. This is racy, but this is fine because both
      // cases should work in the end
      Thread.sleep(10)

      source.runWith(Sink.seq).futureValue should ===(Nil)
    }

    "remember completion for materialisations after completion" in {

      val (sourceProbe, source) = TestSource[Unit]().toMat(BroadcastHub.sink)(Keep.both).run()
      val sinkProbe = source.runWith(TestSink[Unit]())

      sourceProbe.sendComplete()

      sinkProbe.request(1)
      sinkProbe.expectComplete()

      // Materialize a second time. There was a race here, where we managed to enqueue our Source registration just
      // immediately before the Hub shut down.
      val sink2Probe = source.runWith(TestSink[Unit]())

      sink2Probe.request(1)
      sink2Probe.expectComplete()
    }

    "properly signal error to consumers arriving after producer finished" in {
      val source = Source.failed(TE("Fail!")).runWith(BroadcastHub.sink(8))
      // Wait enough so the Hub gets the completion. This is racy, but this is fine because both
      // cases should work in the end
      Thread.sleep(10)

      a[TE] shouldBe thrownBy {
        Await.result(source.runWith(Sink.seq), 3.seconds)
      }
    }

    "handle cancelled Sink" in {
      val in = TestPublisher.probe[Int]()
      val hubSource = Source.fromPublisher(in).runWith(BroadcastHub.sink(4))

      val out = TestSubscriber.probe[Int]()

      hubSource.runWith(Sink.cancelled)
      hubSource.runWith(Sink.fromSubscriber(out))

      out.ensureSubscription()

      out.request(10)
      in.expectRequest()
      in.sendNext(1)
      out.expectNext(1)
      in.sendNext(2)
      out.expectNext(2)
      in.sendNext(3)
      out.expectNext(3)
      in.sendNext(4)
      out.expectNext(4)
      in.sendNext(5)
      out.expectNext(5)

      in.sendComplete()
      out.expectComplete()
    }

  }

  "PartitionHub" must {

    "work in the happy case with one stream" in {
      val source =
        Source(1 to 10).runWith(PartitionHub.sink((_, _) => 0, startAfterNrOfConsumers = 0, bufferSize = 8))
      source.runWith(Sink.seq).futureValue should ===(1 to 10)
    }

    "work in the happy case with two streams" in {
      val source = Source(0 until 10)
        .runWith(PartitionHub.sink((size, elem) => elem % size, startAfterNrOfConsumers = 2, bufferSize = 8))
      val result1 = source.runWith(Sink.seq)
      // it should not start publishing until startAfterNrOfConsumers = 2
      Thread.sleep(20)
      val result2 = source.runWith(Sink.seq)
      result1.futureValue should ===(0 to 8 by 2)
      result2.futureValue should ===(1 to 9 by 2)
    }

    "be able to use as round-robin router" in {
      val source = Source(0 until 10).runWith(PartitionHub.statefulSink(() => {
        var n = 0L

        (info, _) => {
          n += 1
          info.consumerIdByIdx((n % info.size).toInt)
        }
      }, startAfterNrOfConsumers = 2, bufferSize = 8))
      val result1 = source.runWith(Sink.seq)
      val result2 = source.runWith(Sink.seq)
      result1.futureValue should ===(1 to 9 by 2)
      result2.futureValue should ===(0 to 8 by 2)
    }

    "be able to use as sticky session router" in {
      val source = Source(List("usr-1", "usr-2", "usr-1", "usr-3")).runWith(PartitionHub.statefulSink(() => {
        var sessions = Map.empty[String, Long]
        var n = 0L

        (info, elem) => {
          sessions.get(elem) match {
            case Some(id) if info.consumerIds.exists(_ == id) => id
            case _ =>
              n += 1
              val id = info.consumerIdByIdx((n % info.size).toInt)
              sessions = sessions.updated(elem, id)
              id
          }
        }
      }, startAfterNrOfConsumers = 2, bufferSize = 8))
      val result1 = source.runWith(Sink.seq)
      val result2 = source.runWith(Sink.seq)
      result1.futureValue should ===(List("usr-2"))
      result2.futureValue should ===(List("usr-1", "usr-1", "usr-3"))
    }

    "be able to use as fastest consumer router" in {
      val source = Source(0 until 1000).runWith(
        PartitionHub.statefulSink(
          () => (info, _) => info.consumerIds.toVector.minBy(id => info.queueSize(id)),
          startAfterNrOfConsumers = 2,
          bufferSize = 4))
      val result1 = source.runWith(Sink.seq)
      val result2 = source.throttle(10, 100.millis, 10, ThrottleMode.Shaping).runWith(Sink.seq)

      result1.futureValue.size should be > (result2.futureValue.size)
    }

    "route evenly" in {
      val (testSource, hub) = TestSource[Int]()
        .toMat(PartitionHub.sink((size, elem) => elem % size, startAfterNrOfConsumers = 2, bufferSize = 8))(Keep.both)
        .run()
      val probe0 = hub.runWith(TestSink[Int]())
      val probe1 = hub.runWith(TestSink[Int]())
      probe0.request(3)
      probe1.request(10)
      testSource.sendNext(0)
      probe0.expectNext(0)
      testSource.sendNext(1)
      probe1.expectNext(1)

      testSource.sendNext(2)
      testSource.sendNext(3)
      testSource.sendNext(4)
      probe0.expectNext(2)
      probe1.expectNext(3)
      probe0.expectNext(4)

      // probe1 has not requested more
      testSource.sendNext(5)
      testSource.sendNext(6)
      testSource.sendNext(7)
      probe1.expectNext(5)
      probe1.expectNext(7)
      probe0.expectNoMessage(10.millis)
      probe0.request(10)
      probe0.expectNext(6)

      testSource.sendComplete()
      probe0.expectComplete()
      probe1.expectComplete()
    }

    "route unevenly" in {
      val (testSource, hub) = TestSource[Int]()
        .toMat(PartitionHub.sink((_, elem) => (elem % 3) % 2, startAfterNrOfConsumers = 2, bufferSize = 8))(Keep.both)
        .run()
      val probe0 = hub.runWith(TestSink[Int]())
      val probe1 = hub.runWith(TestSink[Int]())

      // (_ % 3) % 2
      // 0 => 0
      // 1 => 1
      // 2 => 0
      // 3 => 0
      // 4 => 1

      probe0.request(10)
      probe1.request(10)
      testSource.sendNext(0)
      probe0.expectNext(0)
      testSource.sendNext(1)
      probe1.expectNext(1)
      testSource.sendNext(2)
      probe0.expectNext(2)
      testSource.sendNext(3)
      probe0.expectNext(3)
      testSource.sendNext(4)
      probe1.expectNext(4)

      testSource.sendComplete()
      probe0.expectComplete()
      probe1.expectComplete()
    }

    "backpressure" in {
      val (testSource, hub) = TestSource[Int]()
        .toMat(PartitionHub.sink((_, _) => 0, startAfterNrOfConsumers = 2, bufferSize = 4))(Keep.both)
        .run()
      val probe0 = hub.runWith(TestSink[Int]())
      val probe1 = hub.runWith(TestSink[Int]())
      probe0.request(10)
      probe1.request(10)
      testSource.sendNext(0)
      probe0.expectNext(0)
      testSource.sendNext(1)
      probe0.expectNext(1)
      testSource.sendNext(2)
      probe0.expectNext(2)
      testSource.sendNext(3)
      probe0.expectNext(3)
      testSource.sendNext(4)
      probe0.expectNext(4)

      testSource.sendComplete()
      probe0.expectComplete()
      probe1.expectComplete()
    }

    "ensure that from two different speed consumers the slower controls the rate" in {
      val (firstElem, source) = Source
        .maybe[Int]
        .concat(Source(1 until 20))
        .toMat(PartitionHub.sink((size, elem) => elem % size, startAfterNrOfConsumers = 2, bufferSize = 1))(Keep.both)
        .run()

      val f1 = source.throttle(1, 10.millis, 1, ThrottleMode.shaping).runWith(Sink.seq)
      // Second cannot be overwhelmed since the first one throttles the overall rate, and second allows a higher rate
      val f2 = source.throttle(10, 10.millis, 8, ThrottleMode.enforcing).runWith(Sink.seq)

      // Ensure subscription of Sinks. This is racy but there is no event we can hook into here.
      Thread.sleep(100)
      firstElem.success(Some(0))
      f1.futureValue should ===(0 to 18 by 2)
      f2.futureValue should ===(1 to 19 by 2)

    }

    "properly signal error to consumers" in {
      val upstream = TestPublisher.probe[Int]()
      val source = Source
        .fromPublisher(upstream)
        .runWith(PartitionHub.sink((size, elem) => elem % size, startAfterNrOfConsumers = 2, bufferSize = 8))

      val downstream1 = TestSubscriber.probe[Int]()
      source.runWith(Sink.fromSubscriber(downstream1))
      val downstream2 = TestSubscriber.probe[Int]()
      source.runWith(Sink.fromSubscriber(downstream2))

      downstream1.request(4)
      downstream2.request(8)

      // to make sure downstream subscriptions are done before
      // starting to send elements
      Thread.sleep(100)

      (0 until 16).foreach(upstream.sendNext(_))

      downstream1.expectNext(0, 2, 4, 6) // 8, 10, 12, 14 stays in buffer bc no demand
      // if we don't demand more from downstream 1 here, there is a race condition where
      // demand from downstream 2 is lost, because the buffer was full with elements for the other partition
      // however this is likely because of the very fine grained demand logic in this test and not likely
      // to happen in reality where both downstreams are likely going to keep pulling, or canceling
      // for a scenario where one dosntream continous back pressures, head of line blocking can anyway happen
      downstream1.request(1)
      downstream2.expectNext(1, 3, 5, 7, 9, 11, 13, 15)
      downstream1.expectNext(8)

      downstream1.expectNoMessage(100.millis)
      downstream2.expectNoMessage(100.millis)

      upstream.sendError(TE("Failed"))

      downstream1.expectError(TE("Failed"))
      downstream2.expectError(TE("Failed"))
    }

    "properly signal completion to consumers arriving after producer finished" in {
      val source =
        Source.empty[Int].runWith(PartitionHub.sink((size, elem) => elem % size, startAfterNrOfConsumers = 0))
      // Wait enough so the Hub gets the completion. This is racy, but this is fine because both
      // cases should work in the end
      Thread.sleep(10)

      source.runWith(Sink.seq).futureValue should ===(Nil)
    }

    "remember completion for materialisations after completion" in {

      val (sourceProbe, source) =
        TestSource[Unit]().toMat(PartitionHub.sink((_, _) => 0, startAfterNrOfConsumers = 0))(Keep.both).run()
      val sinkProbe = source.runWith(TestSink[Unit]())

      sourceProbe.sendComplete()

      sinkProbe.request(1)
      sinkProbe.expectComplete()

      // Materialize a second time. There was a race here, where we managed to enqueue our Source registration just
      // immediately before the Hub shut down.
      val sink2Probe = source.runWith(TestSink[Unit]())

      sink2Probe.request(1)
      sink2Probe.expectComplete()
    }

    "properly signal error to consumers arriving after producer finished" in {
      val source =
        Source.failed[Int](TE("Fail!")).runWith(PartitionHub.sink((_, _) => 0, startAfterNrOfConsumers = 0))
      // Wait enough so the Hub gets the failure. This is racy, but this is fine because both
      // cases should work in the end
      Thread.sleep(10)

      a[TE] shouldBe thrownBy {
        Await.result(source.runWith(Sink.seq), 3.seconds)
      }
    }

    "drop elements with negative index" in {
      val source = Source(0 until 10).runWith(
        PartitionHub.sink(
          (size, elem) => if (elem == 3 || elem == 4) -1 else elem % size,
          startAfterNrOfConsumers = 2,
          bufferSize = 8))
      val result1 = source.runWith(Sink.seq)
      val result2 = source.runWith(Sink.seq)
      result1.futureValue should ===((0 to 8 by 2).filterNot(_ == 4))
      result2.futureValue should ===((1 to 9 by 2).filterNot(_ == 3))
    }

  }

}
