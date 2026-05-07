/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NoStackTrace

import akka.Done
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream.Supervision.stoppingDecider
import akka.stream.testkit.StreamSpec
import akka.testkit.TestLatch
import akka.testkit.TestProbe

class SinkForeachAsyncSpec extends StreamSpec {

  "A foreachAsync" must {
    "handle empty source" in {
      import system.dispatcher
      val p = Source(List.empty[Int]).runWith(Sink.foreachAsync(3)(_ => Future {}))
      Await.result(p, remainingOrDefault)
    }

    "be able to run elements in parallel" in {
      implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

      val probe = TestProbe()
      val latch = (1 to 4).map(_ -> TestLatch(1)).toMap

      val sink: Sink[Int, Future[Done]] = {
        Sink.foreachAsync(4) { (n: Int) =>
          Future {
            Await.result(latch(n), remainingOrDefault)
            probe.ref ! n
          }
        }
      }

      val p = Source(1 to 4).runWith(sink)

      latch(1).countDown()
      probe.expectMsg(1)
      latch(2).countDown()
      probe.expectMsg(2)
      latch(3).countDown()
      probe.expectMsg(3)
      latch(4).countDown()
      probe.expectMsg(4)

      Await.result(p, 4.seconds)
      assert(p.isCompleted)
    }

    "back-pressure upstream elements when downstream is slow" in {
      import scala.concurrent.duration._

      implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

      val probe = TestProbe()
      val latch = (1 to 4).map(_ -> TestLatch(1)).toMap

      val sink: Sink[() => Int, Future[Done]] = {
        Sink.foreachAsync(1) { (n: () => Int) =>
          Future {
            Await.result(latch(n()), remainingOrDefault)
            probe.ref ! n()
            Thread.sleep(2000)
          }
        }
      }

      @volatile var oneCalled = false
      @volatile var twoCalled = false
      @volatile var threeCalled = false
      @volatile var fourCalled = false

      def one = {
        oneCalled = true; 1
      }

      def two = {
        twoCalled = true; 2
      }

      def three = {
        threeCalled = true; 3
      }

      def four = {
        fourCalled = true; 4
      }

      val p =
        Source(List(() => one, () => two, () => three, () => four)).runWith(sink)

      latch(1).countDown()
      probe.expectMsg(1)

      assert(!twoCalled)
      assert(!threeCalled)
      assert(!fourCalled)

      probe.expectNoMessage(2 seconds)

      latch(2).countDown()
      probe.expectMsg(2)

      assert(!threeCalled)
      assert(!fourCalled)

      probe.expectNoMessage(2 seconds)

      latch(3).countDown()
      probe.expectMsg(3)

      assert(!fourCalled)

      probe.expectNoMessage(2 seconds)

      latch(4).countDown()
      probe.expectMsg(4)

      Await.result(p, 4.seconds)
      assert(p.isCompleted)

      assert(oneCalled)
      assert(twoCalled)
      assert(threeCalled)
      assert(fourCalled)
    }
  }

  "produce elements in the order they are ready" in {
    import system.dispatcher

    val probe = TestProbe()
    val latch = (1 to 4).map(_ -> TestLatch(1)).toMap
    val p = Source(1 to 4).runWith(Sink.foreachAsync(4)((n: Int) => {
      Future {
        Await.ready(latch(n), 5.seconds)
        probe.ref ! n
      }
    }))
    latch(2).countDown()
    probe.expectMsg(2)
    latch(4).countDown()
    probe.expectMsg(4)
    latch(3).countDown()
    probe.expectMsg(3)

    assert(!p.isCompleted)

    latch(1).countDown()
    probe.expectMsg(1)

    Await.result(p, 4.seconds)
    assert(p.isCompleted)
  }

  "not run more functions in parallel then specified" in {
    import system.dispatcher

    val probe = TestProbe()
    val latch = (1 to 5).map(_ -> TestLatch()).toMap

    val p = Source(1 to 5).runWith(Sink.foreachAsync(4)((n: Int) => {
      Future {
        probe.ref ! n
        Await.ready(latch(n), 5.seconds)
      }
    }))
    probe.expectMsgAllOf(1, 2, 3, 4)
    probe.expectNoMessage(200.millis)

    assert(!p.isCompleted)

    for (i <- 1 to 4) latch(i).countDown()

    latch(5).countDown()
    probe.expectMsg(5)

    Await.result(p, 5.seconds)
    assert(p.isCompleted)

  }

  "resume after failed future" in {
    import system.dispatcher

    val probe = TestProbe()
    val latch = TestLatch(1)

    val p = Source(1 to 5).runWith(
      Sink
        .foreachAsync(4)((n: Int) => {
          Future {
            if (n == 3) throw new RuntimeException("err1") with NoStackTrace
            else {
              probe.ref ! n
              Await.ready(latch, 10.seconds)
            }
          }
        })
        .withAttributes(supervisionStrategy(resumingDecider)))

    latch.countDown()
    probe.expectMsgAllOf(1, 2, 4, 5)

    Await.result(p, 5.seconds)
  }

  "finish after failed future" in {
    import system.dispatcher

    val probe = TestProbe()
    val element4Latch = new CountDownLatch(1)
    val errorLatch = new CountDownLatch(2)

    val p = Source
      .fromIterator(() => Iterator.from(1))
      .runWith(
        Sink
          .foreachAsync(3)((n: Int) => {
            Future {
              if (n == 3) {
                // Error will happen only after elements 1, 2 has been processed
                await(errorLatch)
                throw new RuntimeException("err2") with NoStackTrace
              } else {
                probe.ref ! n
                errorLatch.countDown()
                await(element4Latch) // Block element 4, 5, 6, ... from entering
              }
            }
          })
          .withAttributes(supervisionStrategy(stoppingDecider)))

    // Only the first two messages are guaranteed to arrive due to their enforced ordering related to the time
    // of failure.
    probe.expectMsgAllOf(1, 2)
    element4Latch.countDown() // Release elements 4, 5, 6, ...

    a[RuntimeException] must be thrownBy Await.result(p, 3.seconds)
  }

  def await(latch: CountDownLatch): Unit =
    latch.await(5, TimeUnit.SECONDS)
}
