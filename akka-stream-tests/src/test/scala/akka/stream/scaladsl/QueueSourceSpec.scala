/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn
import scala.concurrent._
import scala.concurrent.duration._

import org.scalatest.time.Span

import akka.Done
import akka.actor.Status
import akka.pattern.pipe
import akka.stream._
import akka.stream.impl.QueueSource
import akka.stream.testkit.GraphStageMessages
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSourceStage
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe

class QueueSourceSpec extends StreamSpec {
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  val pause = 300.millis

  // more frequent checks than defaults from AkkaSpec
  implicit val testPatience: PatienceConfig = {
    import akka.testkit.TestDuration
    PatienceConfig(testKitSettings.DefaultTimeout.duration.dilated, Span(5, org.scalatest.time.Millis))
  }

  def assertSuccess(f: Future[QueueOfferResult]): Unit = {
    f.futureValue should ===(QueueOfferResult.Enqueued)
  }

  "A QueueSource" must {

    "emit received messages to the stream" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(10, OverflowStrategy.fail).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()
      for (i <- 1 to 3) {
        sub.request(1)
        assertSuccess(queue.offer(i))
        s.expectNext(i)
      }

      queue.watchCompletion().pipeTo(testActor)
      expectNoMessage(pause)

      sub.cancel()
      expectMsg(Done)
    }

    "be reusable" in {
      val source = Source.queue(0, OverflowStrategy.backpressure)
      val q1 = source.to(Sink.ignore).run()
      q1.complete()
      q1.watchCompletion().futureValue should ===(Done)
      val q2 = source.to(Sink.ignore).run()
      q2.watchCompletion().value should ===(None)
    }

    "reject elements when back-pressuring with maxBuffer=0" in {
      val (source, probe) = Source.queue[Int](0, OverflowStrategy.backpressure).toMat(TestSink())(Keep.both).run()
      val f = source.offer(42)
      val ex = source.offer(43).failed.futureValue
      ex shouldBe a[IllegalStateException]
      ex.getMessage should include("have to wait")
      probe.requestNext() should ===(42)
      f.futureValue should ===(QueueOfferResult.Enqueued)
    }

    "reject elements when completed" in {
      // Not using the materialized test sink leads to the 42 being enqueued but not emitted due to lack of demand.
      // This will also not effectively complete the stream, hence there is enough time (no races) to offer 43
      // and verify it is dropped.
      val source = Source.queue[Int](42, OverflowStrategy.backpressure).to(TestSink()).run()
      source.offer(42)
      source.complete()
      val f = source.offer(43)
      f.futureValue should ===(QueueOfferResult.Dropped)
    }

    "buffer when needed" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(100, OverflowStrategy.dropHead).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()
      for (n <- 1 to 20) assertSuccess(queue.offer(n))
      sub.request(10)
      for (n <- 1 to 10) assertSuccess(queue.offer(n))
      sub.request(10)
      for (n <- 11 to 20) assertSuccess(queue.offer(n))

      for (n <- 200 to 399) assertSuccess(queue.offer(n))
      sub.request(100)
      for (n <- 300 to 399) assertSuccess(queue.offer(n))
      sub.cancel()
    }

    "not fail when 0 buffer space and demand is signalled" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(0, OverflowStrategy.dropHead).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()
      sub.request(1)

      assertSuccess(queue.offer(1))

      sub.cancel()
    }

    "wait for demand when buffer is 0" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(0, OverflowStrategy.dropHead).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()
      queue.offer(1).pipeTo(testActor)
      expectNoMessage(pause)
      sub.request(1)
      expectMsg(QueueOfferResult.Enqueued)
      s.expectNext(1)
      sub.cancel()
    }

    "finish offer and complete futures when stream completed" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(0, OverflowStrategy.dropHead).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()

      queue.watchCompletion().pipeTo(testActor)
      queue.offer(1).pipeTo(testActor)
      expectNoMessage(pause)

      sub.cancel()

      expectMsgAllOf(QueueOfferResult.QueueClosed, Done)
    }

    "fail future immediately when stream is already cancelled" in {
      val queue = Source.queue[Int](0, OverflowStrategy.dropHead).to(Sink.cancelled).run()
      queue.watchCompletion().futureValue
      queue.offer(1).failed.futureValue shouldBe a[StreamDetachedException]
    }

    "fail stream on buffer overflow in fail mode" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(1, OverflowStrategy.fail).to(Sink.fromSubscriber(s)).run()
      s.expectSubscription()

      queue.offer(1)
      queue.offer(2)
      s.expectError()
    }

    "remember pull from downstream to send offered element immediately" in {
      val s = TestSubscriber.manualProbe[Int]()
      val probe = TestProbe()
      val queue =
        TestSourceStage(new QueueSource[Int](1, OverflowStrategy.dropHead, 1), probe).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()

      sub.request(1)
      probe.expectMsg(GraphStageMessages.Pull)
      assertSuccess(queue.offer(1))
      s.expectNext(1)
      sub.cancel()
    }

    "fail the second (concurrent) offer in backpressure mode with default maxConcurrentPulls" in {
      val (queue, probe) = Source.queue[Int](5, OverflowStrategy.backpressure).toMat(TestSink())(Keep.both).run()

      for (i <- 1 to 5) assertSuccess(queue.offer(i))

      queue.offer(6).pipeTo(testActor)

      queue.offer(7).pipeTo(testActor)
      expectMsgType[Status.Failure].cause shouldBe an[IllegalStateException]

      probe.requestNext(1)
      expectMsg(QueueOfferResult.Enqueued)
      queue.complete()

      probe.request(6).expectNext(2, 3, 4, 5, 6).expectComplete()
    }

    "allow to wait `n` offer futures in backpressure mode with `n` maxConcurrentPulls" in {
      val n = 2
      val (queue, probe) = Source.queue[Int](5, OverflowStrategy.backpressure, n).toMat(TestSink())(Keep.both).run()

      for (i <- 1 to 5) assertSuccess(queue.offer(i))

      queue.offer(6).pipeTo(testActor)
      queue.offer(7).pipeTo(testActor)
      queue.offer(8).pipeTo(testActor)
      expectMsgType[Status.Failure].cause shouldBe an[IllegalStateException]

      probe.requestNext(1)
      expectMsg(QueueOfferResult.Enqueued)

      probe.requestNext(2)
      expectMsg(QueueOfferResult.Enqueued)

      queue.complete()

      probe.request(7).expectNext(3, 4, 5, 6, 7).expectComplete()
    }

    "complete watching future with failure if stream failed" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(1, OverflowStrategy.fail).to(Sink.fromSubscriber(s)).run()
      queue.watchCompletion().pipeTo(testActor)
      queue.offer(1) //need to wait when first offer is done as initialization can be done in this moment
      queue.offer(2)
      expectMsgClass(classOf[Status.Failure])
    }

    "complete watching future with failure if materializer shut down" in {
      val tempMap = Materializer(system)
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(1, OverflowStrategy.fail).to(Sink.fromSubscriber(s)).run()(tempMap)
      queue.watchCompletion().pipeTo(testActor)
      tempMap.shutdown()
      expectMsgClass(classOf[Status.Failure])
    }

    "return false when element was not added to buffer" in {
      val s = TestSubscriber.manualProbe[Int]()
      @nowarn("msg=deprecated")
      val queue = Source.queue(1, OverflowStrategy.dropNew).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()

      queue.offer(1)
      queue.offer(2).pipeTo(testActor)
      expectMsg(QueueOfferResult.Dropped)

      sub.request(1)
      s.expectNext(1)
      sub.cancel()
    }

    "wait when buffer is full and backpressure is on" in {
      val s = TestSubscriber.manualProbe[Int]()
      val queue = Source.queue(1, OverflowStrategy.backpressure).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()
      assertSuccess(queue.offer(1))

      queue.offer(2).pipeTo(testActor)
      expectNoMessage(pause)

      sub.request(1)
      s.expectNext(1)

      sub.request(1)
      s.expectNext(2)
      expectMsg(QueueOfferResult.Enqueued)

      sub.cancel()
    }

    "fail offer future when stream is completed" in {
      val s = TestSubscriber.manualProbe[Int]()
      @nowarn("msg=deprecated")
      val queue = Source.queue(1, OverflowStrategy.dropNew).to(Sink.fromSubscriber(s)).run()
      val sub = s.expectSubscription()
      queue.watchCompletion().pipeTo(testActor)
      sub.cancel()
      expectMsg(Done)

      queue.offer(1).failed.futureValue shouldBe an[StreamDetachedException]
    }

    "not share future across materializations" in {
      val source = Source.queue[String](1, OverflowStrategy.fail)

      val mat1subscriber = TestSubscriber.probe[String]()
      val mat2subscriber = TestSubscriber.probe[String]()
      val sourceQueue1 = source.to(Sink.fromSubscriber(mat1subscriber)).run()
      val sourceQueue2 = source.to(Sink.fromSubscriber(mat2subscriber)).run()

      mat1subscriber.ensureSubscription()
      mat2subscriber.ensureSubscription()

      mat1subscriber.request(1)
      sourceQueue1.offer("hello")
      mat1subscriber.expectNext("hello")
      mat1subscriber.cancel()
      sourceQueue1.watchCompletion().pipeTo(testActor)
      expectMsg(Done)

      sourceQueue2.watchCompletion().isCompleted should ===(false)
    }

    "complete the stream" when {

      "buffer is empty" in {
        val (source, probe) = Source.queue[Int](1, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.complete()
        source.watchCompletion().futureValue should ===(Done)
        probe.ensureSubscription().expectComplete()
      }

      "buffer is full" in {
        val (source, probe) = Source.queue[Int](1, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.offer(1)
        source.complete()
        probe.requestNext(1).expectComplete()
        source.watchCompletion().futureValue should ===(Done)
      }

      "buffer is full and element is pending" in {
        val (source, probe) = Source.queue[Int](1, OverflowStrategy.backpressure).toMat(TestSink())(Keep.both).run()
        source.offer(1)
        source.offer(2)
        source.complete()
        probe.requestNext(1).requestNext(2).expectComplete()
        source.watchCompletion().futureValue should ===(Done)
      }

      "no buffer is used" in {
        val (source, probe) = Source.queue[Int](0, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.complete()
        source.watchCompletion().futureValue should ===(Done)
        probe.ensureSubscription().expectComplete()
      }

      "no buffer is used and element is pending" in {
        val (source, probe) = Source.queue[Int](0, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.offer(1)
        source.complete()
        probe.requestNext(1).expectComplete()
        source.watchCompletion().futureValue should ===(Done)
      }

      "some elements not yet delivered to stage" in {
        val (queue, _) =
          Source.queue[Unit](10, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        intercept[StreamDetachedException] {
          Await.result((1 to 15).map(_ => queue.offer(())).last, 3.seconds)
        }
      }
    }

    "fail the stream" when {
      val ex = new Exception("BUH")

      "buffer is empty" in {
        val (source, probe) = Source.queue[Int](1, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.fail(ex)
        source.watchCompletion().failed.futureValue should ===(ex)
        probe.ensureSubscription().expectError(ex)
      }

      "buffer is full" in {
        val (source, probe) = Source.queue[Int](1, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.offer(1)
        source.fail(ex)
        source.watchCompletion().failed.futureValue should ===(ex)
        probe.ensureSubscription().expectError(ex)
      }

      "buffer is full and element is pending" in {
        val (source, probe) = Source.queue[Int](1, OverflowStrategy.backpressure).toMat(TestSink())(Keep.both).run()
        source.offer(1)
        source.offer(2)
        source.fail(ex)
        source.watchCompletion().failed.futureValue should ===(ex)
        probe.ensureSubscription().expectError(ex)
      }

      "no buffer is used" in {
        val (source, probe) = Source.queue[Int](0, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.fail(ex)
        source.watchCompletion().failed.futureValue should ===(ex)
        probe.ensureSubscription().expectError(ex)
      }

      "no buffer is used and element is pending" in {
        val (source, probe) = Source.queue[Int](0, OverflowStrategy.fail).toMat(TestSink())(Keep.both).run()
        source.offer(1)
        source.fail(ex)
        source.watchCompletion().failed.futureValue should ===(ex)
        probe.ensureSubscription().expectError(ex)
      }
    }

    "materialize to a queue which is seamlessly translatable between scala and java DSL" in {
      val s = TestSubscriber.manualProbe[Int]()

      val javadslQueue = Source.queue(10, OverflowStrategy.fail).to(Sink.fromSubscriber(s)).run().asJava
      val scaladslQueue = akka.stream.javadsl.SourceQueueWithComplete.asScala(javadslQueue)
      val sub = s.expectSubscription()

      sub.request(1)
      assertSuccess(scaladslQueue.offer(42))
      s.expectNext(42)

      scaladslQueue.watchCompletion().pipeTo(testActor)
      expectNoMessage(pause)

      sub.cancel()
      expectMsg(Done)
    }

  }

}
