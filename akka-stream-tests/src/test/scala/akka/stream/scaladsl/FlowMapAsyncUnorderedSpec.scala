/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import org.scalatest.concurrent.PatienceConfiguration.Timeout

import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream.testkit._
import akka.stream.testkit.scaladsl._
import akka.testkit.TestLatch
import akka.testkit.TestProbe

class FlowMapAsyncUnorderedSpec extends StreamSpec {

  "A Flow with mapAsyncUnordered" must {

    "produce future elements in the order they are ready" in {
      val c = TestSubscriber.manualProbe[Int]()
      implicit val ec = system.dispatcher
      val latch = (1 to 4).map(_ -> TestLatch(1)).toMap
      Source(1 to 4)
        .mapAsyncUnordered(4)(n =>
          Future {
            Await.ready(latch(n), 5.seconds)
            n
          })
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(5)
      latch(2).countDown()
      c.expectNext(2)
      latch(4).countDown()
      c.expectNext(4)
      latch(3).countDown()
      c.expectNext(3)
      latch(1).countDown()
      c.expectNext(1)
      c.expectComplete()
    }

    "complete without requiring further demand (parallelism = 1)" in {
      import system.dispatcher
      Source
        .single(1)
        .mapAsyncUnordered(1)(v => Future { Thread.sleep(20); v })
        .runWith(TestSink[Int]())
        .requestNext(1)
        .expectComplete()
    }

    "complete without requiring further demand with already completed future (parallelism = 1)" in {
      Source
        .single(1)
        .mapAsyncUnordered(1)(v => Future.successful(v))
        .runWith(TestSink[Int]())
        .requestNext(1)
        .expectComplete()
    }

    "complete without requiring further demand (parallelism = 2)" in {
      import system.dispatcher
      val probe =
        Source(1 :: 2 :: Nil).mapAsyncUnordered(2)(v => Future { Thread.sleep(20); v }).runWith(TestSink[Int]())

      probe.request(2).expectNextN(2)
      probe.expectComplete()
    }

    "complete without requiring further demand with already completed future (parallelism = 2)" in {
      val probe = Source(1 :: 2 :: Nil).mapAsyncUnordered(2)(v => Future.successful(v)).runWith(TestSink[Int]())

      probe.request(2).expectNextN(2)
      probe.expectComplete()
    }

    "not run more futures than requested elements" in {
      val probe = TestProbe()
      val c = TestSubscriber.manualProbe[Int]()
      implicit val ec = system.dispatcher
      Source(1 to 20)
        .mapAsyncUnordered(4)(n =>
          if (n % 3 == 0) {
            probe.ref ! n
            Future.successful(n)
          } else
            Future {
              probe.ref ! n
              n
            })
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      c.expectNoMessage(200.millis)
      probe.expectNoMessage(Duration.Zero)
      sub.request(1)
      var got = Set(c.expectNext())
      probe.expectMsgAllOf(1, 2, 3, 4, 5)
      probe.expectNoMessage(500.millis)
      sub.request(25)
      probe.expectMsgAllOf(6 to 20: _*)
      c.within(3.seconds) {
        for (_ <- 2 to 20) got += c.expectNext()
      }

      got should be((1 to 20).toSet)
      c.expectComplete()
    }

    "signal future failure" in {
      val latch = TestLatch(1)
      val c = TestSubscriber.manualProbe[Int]()
      implicit val ec = system.dispatcher
      Source(1 to 5)
        .mapAsyncUnordered(4)(n =>
          Future {
            if (n == 3) throw new RuntimeException("err1") with NoStackTrace
            else {
              Await.ready(latch, 10.seconds)
              n
            }
          })
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectError().getMessage should be("err1")
      latch.countDown()
    }

    "signal future failure asap" in {
      val latch = TestLatch(1)
      val done = Source(1 to 5)
        .map { n =>
          if (n == 1) n
          else {
            // slow upstream should not block the error
            Await.ready(latch, 10.seconds)
            n
          }
        }
        .mapAsyncUnordered(4) { n =>
          if (n == 1) Future.failed(new RuntimeException("err1") with NoStackTrace)
          else Future.successful(n)
        }
        .runWith(Sink.ignore)
      intercept[RuntimeException] {
        Await.result(done, remainingOrDefault)
      }.getMessage should be("err1")
      latch.countDown()
    }

    "signal error from mapAsyncUnordered" in {
      val latch = TestLatch(1)
      val c = TestSubscriber.manualProbe[Int]()
      implicit val ec = system.dispatcher
      Source(1 to 5)
        .mapAsyncUnordered(4)(n =>
          if (n == 3) throw new RuntimeException("err2") with NoStackTrace
          else {
            Future {
              Await.ready(latch, 10.seconds)
              n
            }
          })
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectError().getMessage should be("err2")
      latch.countDown()
    }

    "resume after future failure" in {
      implicit val ec = system.dispatcher
      Source(1 to 5)
        .mapAsyncUnordered(4)(n =>
          Future {
            if (n == 3) throw new RuntimeException("err3") with NoStackTrace
            else n
          })
        .withAttributes(supervisionStrategy(resumingDecider))
        .runWith(TestSink[Int]())
        .request(10)
        .expectNextUnordered(1, 2, 4, 5)
        .expectComplete()
    }

    "resume after multiple failures" in {
      val futures: List[Future[String]] = List(
        Future.failed(Utils.TE("failure1")),
        Future.failed(Utils.TE("failure2")),
        Future.failed(Utils.TE("failure3")),
        Future.failed(Utils.TE("failure4")),
        Future.failed(Utils.TE("failure5")),
        Future.successful("happy!"))

      Await.result(
        Source(futures)
          .mapAsyncUnordered(2)(identity)
          .withAttributes(supervisionStrategy(resumingDecider))
          .runWith(Sink.head),
        3.seconds) should ===("happy!")
    }

    "finish after future failure" in {
      import system.dispatcher
      Await.result(
        Source(1 to 3)
          .mapAsyncUnordered(1)(n =>
            Future {
              if (n == 3) throw new RuntimeException("err3b") with NoStackTrace
              else n
            })
          .withAttributes(supervisionStrategy(resumingDecider))
          .grouped(10)
          .runWith(Sink.head),
        1.second) should be(Seq(1, 2))
    }

    "resume when mapAsyncUnordered throws" in {
      implicit val ec = system.dispatcher
      Source(1 to 5)
        .mapAsyncUnordered(4)(n =>
          if (n == 3) throw new RuntimeException("err4") with NoStackTrace
          else Future(n))
        .withAttributes(supervisionStrategy(resumingDecider))
        .runWith(TestSink[Int]())
        .request(10)
        .expectNextUnordered(1, 2, 4, 5)
        .expectComplete()
    }

    "ignore element when future is completed with null" in {
      val flow = Flow[Int].mapAsyncUnordered[String](2) {
        case 2 => Future.successful(null)
        case x => Future.successful(x.toString)
      }
      val result = Source(List(1, 2, 3)).via(flow).runWith(Sink.seq)

      result.futureValue should contain.only("1", "3")
    }

    "continue emitting after a sequence of nulls" in {
      val flow = Flow[Int].mapAsyncUnordered[String](3) { value =>
        if (value == 0 || value >= 100) Future.successful(value.toString)
        else Future.successful(null)
      }

      val result = Source(0 to 102).via(flow).runWith(Sink.seq)

      result.futureValue should contain.only("0", "100", "101", "102")
    }

    "complete without emitting any element after a sequence of nulls only" in {
      val flow = Flow[Int].mapAsyncUnordered[String](3) { _ =>
        Future.successful(null)
      }

      val result = Source(0 to 200).via(flow).runWith(Sink.seq)

      result.futureValue shouldBe empty
    }

    "complete stage if future with null result is completed last" in {
      import system.dispatcher
      val latch = TestLatch(2)

      val flow = Flow[Int].mapAsyncUnordered[String](2) {
        case 2 =>
          Future {
            Await.ready(latch, 10.seconds)
            null
          }
        case x =>
          latch.countDown()
          Future.successful(x.toString)
      }

      val result = Source(List(1, 2, 3)).via(flow).runWith(Sink.seq)

      result.futureValue should contain.only("1", "3")
    }

    "handle cancel properly" in {
      val pub = TestPublisher.manualProbe[Int]()
      val sub = TestSubscriber.manualProbe[Int]()

      Source.fromPublisher(pub).mapAsyncUnordered(4)(Future.successful).runWith(Sink.fromSubscriber(sub))

      val upstream = pub.expectSubscription()
      upstream.expectRequest()

      sub.expectSubscription().cancel()

      upstream.expectCancellation()

    }

    "not run more futures than configured" in {
      val parallelism = 8

      val counter = new AtomicInteger
      val queue = new LinkedBlockingQueue[(Promise[Int], Long)]

      val timer = new Thread {
        val delay = 50000 // nanoseconds
        var count = 0
        @tailrec final override def run(): Unit = {
          val cont = try {
            val (promise, enqueued) = queue.take()
            val wakeup = enqueued + delay
            while (System.nanoTime() < wakeup) {}
            counter.decrementAndGet()
            promise.success(count)
            count += 1
            true
          } catch {
            case _: InterruptedException => false
          }
          if (cont) run()
        }
      }
      timer.start

      def deferred(): Future[Int] = {
        if (counter.incrementAndGet() > parallelism) Future.failed(new Exception("parallelism exceeded"))
        else {
          val p = Promise[Int]()
          queue.offer(p -> System.nanoTime())
          p.future
        }
      }

      try {
        val N = 10000
        Source(1 to N)
          .mapAsyncUnordered(parallelism)(_ => deferred())
          .runFold(0)((c, _) => c + 1)
          .futureValue(Timeout(3.seconds)) should ===(N)
      } finally {
        timer.interrupt()
      }
    }

  }
}
