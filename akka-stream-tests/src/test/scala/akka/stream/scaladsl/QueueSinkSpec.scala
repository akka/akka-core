/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.pattern.pipe
import akka.stream.AbruptTerminationException
import akka.stream.Attributes.inputBuffer
import akka.stream.Materializer
import akka.stream.StreamDetachedException
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSource

class QueueSinkSpec extends StreamSpec {
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val ex = new RuntimeException("ex") with NoStackTrace

  val noMsgTimeout = 300.millis

  "An QueueSinkSpec" must {

    "send the elements as result of future" in {
      val expected = List(Some(1), Some(2), Some(3), None)
      val queue = Source(expected.flatten).runWith(Sink.queue())
      expected.foreach { v =>
        queue.pull().pipeTo(testActor)
        expectMsg(v)
      }
    }

    "allow to have only one future waiting for result in each point of time with default maxConcurrentOffers" in {
      val probe = TestPublisher.manualProbe[Int]()
      val queue = Source.fromPublisher(probe).runWith(Sink.queue())
      val sub = probe.expectSubscription()
      val future = queue.pull()
      val future2 = queue.pull()
      an[IllegalStateException] shouldBe thrownBy { Await.result(future2, remainingOrDefault) }

      sub.sendNext(1)
      future.pipeTo(testActor)
      expectMsg(Some(1))

      sub.sendComplete()
      queue.pull()
    }

    "allow to have `n` futures waiting for result in each point of time with `n` maxConcurrentOffers" in {
      val n = 2
      val probe = TestPublisher.manualProbe[Int]()
      val queue = Source.fromPublisher(probe).runWith(Sink.queue(n))
      val sub = probe.expectSubscription()
      val future1 = queue.pull()
      val future2 = queue.pull()
      val future3 = queue.pull()
      an[IllegalStateException] shouldBe thrownBy { Await.result(future3, remainingOrDefault) }

      sub.sendNext(1)
      future1.pipeTo(testActor)
      expectMsg(Some(1))

      sub.sendNext(2)
      future2.pipeTo(testActor)
      expectMsg(Some(2))

      sub.sendComplete()
      queue.pull()
    }

    "fail all futures on abrupt termination" in {
      val n = 2
      val mat = Materializer(system)
      val queue = TestSource().runWith(Sink.queue(n))(mat)

      val future1 = queue.pull()
      val future2 = queue.pull()
      mat.shutdown()

      // async callback can be executed after materializer shutdown so you should also expect StreamDetachedException
      val fail1 = future1.failed.futureValue
      val fail2 = future2.failed.futureValue
      assert(fail1.isInstanceOf[AbruptTerminationException] || fail1.isInstanceOf[StreamDetachedException])
      assert(fail2.isInstanceOf[AbruptTerminationException] || fail2.isInstanceOf[StreamDetachedException])
    }

    "complete all futures with None on upstream complete" in {
      val n = 2
      val probe = TestPublisher.probe[Int]()
      val queue = Source.fromPublisher(probe).runWith(Sink.queue(n))
      val future1 = queue.pull()
      val future2 = queue.pull()
      probe.sendComplete()
      future1.futureValue shouldBe None
      future2.futureValue shouldBe None
    }

    "fail all futures on upstream fail" in {
      val n = 2
      val probe = TestPublisher.probe[Int]()
      val queue = Source.fromPublisher(probe).runWith(Sink.queue(n))
      val future1 = queue.pull()
      val future2 = queue.pull()
      val ex = new IllegalArgumentException
      probe.sendError(ex)
      future1.failed.futureValue shouldBe ex
      future2.failed.futureValue shouldBe ex
    }

    "wait for next element from upstream" in {
      val probe = TestPublisher.manualProbe[Int]()
      val queue = Source.fromPublisher(probe).runWith(Sink.queue())
      val sub = probe.expectSubscription()

      queue.pull().pipeTo(testActor)
      expectNoMessage(noMsgTimeout)

      sub.sendNext(1)
      expectMsg(Some(1))
      sub.sendComplete()
      queue.pull()
    }

    "fail future immediately if stream already canceled" in {
      val queue = Source.empty[Int].runWith(Sink.queue())
      // race here because no way to observe that queue sink saw termination
      awaitAssert({
        queue.pull().failed.futureValue shouldBe a[StreamDetachedException]
      })
    }

    "timeout future when stream cannot provide data" in {
      val probe = TestPublisher.manualProbe[Int]()
      val queue = Source.fromPublisher(probe).runWith(Sink.queue())
      val sub = probe.expectSubscription()

      queue.pull().pipeTo(testActor)
      expectNoMessage(noMsgTimeout)

      sub.sendNext(1)
      expectMsg(Some(1))
      sub.sendComplete()
      queue.pull()
    }

    "fail pull future when stream is completed" in {
      val probe = TestPublisher.manualProbe[Int]()
      val queue = Source.fromPublisher(probe).runWith(Sink.queue())
      val sub = probe.expectSubscription()

      queue.pull().pipeTo(testActor)
      sub.sendNext(1)
      expectMsg(Some(1))

      sub.sendComplete()
      Await.result(queue.pull(), noMsgTimeout) should be(None)

      queue.pull().failed.futureValue shouldBe an[StreamDetachedException]
    }

    "keep on sending even after the buffer has been full" in {
      val bufferSize = 16
      val streamElementCount = bufferSize + 4
      val sink = Sink.queue[Int]().withAttributes(inputBuffer(bufferSize, bufferSize))
      val bufferFullProbe = Promise[akka.Done.type]()
      val queue = Source(1 to streamElementCount)
        .alsoTo(Flow[Int].drop(bufferSize - 1).to(Sink.foreach(_ => bufferFullProbe.trySuccess(akka.Done))))
        .toMat(sink)(Keep.right)
        .run()
      bufferFullProbe.future.futureValue should ===(akka.Done)
      for (i <- 1 to streamElementCount) {
        queue.pull().pipeTo(testActor)
        expectMsg(Some(i))
      }
      queue.pull().pipeTo(testActor)
      expectMsg(None)

    }

    "cancel upstream on cancel" in {
      val queue = Source.repeat(1).runWith(Sink.queue())
      queue.pull()
      queue.cancel()
    }

    "be able to cancel upstream right away" in {
      val queue = Source.repeat(1).runWith(Sink.queue())
      queue.cancel()
    }

    "work with one element buffer" in {
      val sink = Sink.queue[Int]().withAttributes(inputBuffer(1, 1))
      val probe = TestPublisher.manualProbe[Int]()
      val queue = Source.fromPublisher(probe).runWith(sink)
      val sub = probe.expectSubscription()

      queue.pull().pipeTo(testActor)
      sub.sendNext(1) // should pull next element
      expectMsg(Some(1))

      queue.pull().pipeTo(testActor)
      expectNoMessage(200.millis) // element requested but buffer empty
      sub.sendNext(2)
      expectMsg(Some(2))

      sub.sendComplete()
      Await.result(queue.pull(), noMsgTimeout) should be(None)
    }

    "fail to materialize with zero sized input buffer" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Source.single(()).runWith(Sink.queue().withAttributes(inputBuffer(0, 0)))
      }
    }

    "materialize to a queue which is seamlessly translatable between scala and java DSL" in {
      val expected = List(Some(1), Some(2), Some(3), None)
      val javadslQueue = Source(expected.flatten).runWith(Sink.queue()).asJava
      val scaladslQueue = akka.stream.javadsl.SinkQueueWithCancel.asScala(javadslQueue)
      expected.foreach { v =>
        scaladslQueue.pull().pipeTo(testActor)
        expectMsg(v)
      }
    }
  }
}
