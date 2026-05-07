/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.scalatest.compatible.Assertion

import akka.Done
import akka.stream.ActorAttributes
import akka.stream.Supervision
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestLatch
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

class FlowMapAsyncPartitionedSpec extends StreamSpec with WithLogCapturing {
  import Utils.TE

  "A Flow with mapAsyncPartitioned" must {
    "produce future elements" in {
      import system.dispatcher

      val c = TestSubscriber.manualProbe[Int]()
      Source(1 to 3)
        .mapAsyncPartitioned(4, 2)(_ % 2) { (elem, _) =>
          Future(elem)
        }
        .runWith(Sink.fromSubscriber(c))

      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext(1)
      c.expectNext(2)
      c.expectNoMessage(200.millis)
      sub.request(2)
      c.expectNext(3)
      c.expectComplete()
    }
  }

  "produce elements in order of ingestion regardless of future completion order" in {
    val c = TestSubscriber.manualProbe[Int]()
    val promises = (0 until 50).map { _ =>
      Promise[Int]()
    }.toArray

    Source(0 until 50)
      .mapAsyncPartitioned(25, 3)(_ % 7) { (elem, _) =>
        promises(elem).future
      }
      .to(Sink.fromSubscriber(c))
      .run()

    val sub = c.expectSubscription()
    sub.request(1000)

    // completes all the promises in random order
    @annotation.tailrec
    def iter(completed: Int, i: Int): Unit =
      if (completed != promises.size) {
        if (promises(i).isCompleted) iter(completed, (i + 1) % promises.size)
        else {
          promises(i).success(i)
          iter(completed + 1, scala.util.Random.nextInt(promises.size))
        }
      }

    iter(0, scala.util.Random.nextInt(promises.size))

    (0 until 50).foreach { i =>
      c.expectNext(i)
    }
    c.expectComplete()
  }

  "not run more futures than overall parallelism" in {
    import system.dispatcher

    val probe = TestProbe()
    val c = TestSubscriber.manualProbe[Int]()

    // parallelism, perPartition, and partitioner chosen to maximize number of futures
    //  in-flight while leaving an opportunity for the stream etc. to still run
    val maxParallelism = (Runtime.getRuntime.availableProcessors - 1).max(3).min(8)
    Source(1 to 22)
      .mapAsyncPartitioned(maxParallelism, 2)(_ % 22) { (elem, _) =>
        Future {
          probe.ref ! elem
          elem
        }
      }
      .to(Sink.fromSubscriber(c))
      .run()

    val sub = c.expectSubscription()
    probe.expectNoMessage(100.millis)
    sub.request(1)

    // theSameElementsAs (viz. ordering insensitive) because the order in which messages are received by the probe
    //  is not deterministic, but the bunching (caused by downstream demand) should be deterministic
    probe.receiveN(maxParallelism + 1) should contain theSameElementsAs (1 to (maxParallelism + 1))
    probe.expectNoMessage(100.millis)
    sub.request(2)
    probe.receiveN(2) should contain theSameElementsAs (2 to 3).map(_ + maxParallelism)
    probe.expectNoMessage(100.millis)
    sub.request(10)
    probe.receiveN(10) should contain theSameElementsAs (4 to 13).map(_ + maxParallelism)
    probe.expectNoMessage(200.millis)
  }

  "not backpressure based on perPartition limit" in {
    val processingProbe = TestProbe()
    case class Elem(n: Int, promise: Promise[Done])
    val (sourceProbe, result) =
      TestSource[Int]()
        .viaMat(Flow[Int].mapAsyncPartitioned(10, 1)(_ < 9) {
          case (n, _) =>
            val promise = Promise[Done]()
            processingProbe.ref ! Elem(n, promise)
            promise.future.map(_ => n)(ExecutionContext.parasitic)
        })(Keep.left)
        .toMat(Sink.seq[Int])(Keep.both)
        .run()

    // we get to send all right away (goes into buffers)
    (0 to 10).foreach(n => sourceProbe.sendNext(n))

    // only these two in flight, based in perPartition
    // partition true
    val elem0 = processingProbe.expectMsgType[Elem]
    elem0.n should ===(0)

    // partition false
    val elem9 = processingProbe.expectMsgType[Elem]
    elem9.n should ===(9)

    processingProbe.expectNoMessage(10.millis) // both perPartition busy

    // unlock partition true, should let us work through all
    elem0.promise.success(Done)
    (1 to 8).foreach { n =>
      val elemN = processingProbe.expectMsgType[Elem]
      elemN.n should ===(n)
      elemN.promise.success(Done)
    }

    // unlock partition false
    elem9.promise.success(Done)
    val elem10 = processingProbe.expectMsgType[Elem]
    elem10.n should ===(10)
    elem10.promise.success(Done)

    sourceProbe.sendComplete()

    // results are in order
    result.futureValue should ===((0 to 10).toVector)
  }

  "signal future already failed" in {
    import system.dispatcher

    val latch = TestLatch(1)
    val c = TestSubscriber.manualProbe[Int]()
    Source(1 to 5)
      .mapAsyncPartitioned(4, 2)(_ % 3) { (elem, _) =>
        if (elem == 3) Future.failed[Int](new TE("BOOM TRES!"))
        else
          Future {
            Await.ready(latch, 10.seconds)
            elem
          }
      }
      .to(Sink.fromSubscriber(c))
      .run()

    val sub = c.expectSubscription()
    sub.request(10)
    c.expectError().getMessage shouldBe "BOOM TRES!"
    latch.countDown()
  }

  "signal future failure" in {
    import system.dispatcher

    val latch = TestLatch(1)
    val c = TestSubscriber.manualProbe[Int]()

    Source(1 to 5)
      .mapAsyncPartitioned(4, 2)(_ % 3) { (elem, _) =>
        Future {
          if (elem == 3) throw new TE("BOOM TROIS!")
          else {
            Await.ready(latch, 10.seconds)
            elem
          }
        }
      }
      .to(Sink.fromSubscriber(c))
      .run()

    val sub = c.expectSubscription()
    sub.request(10)
    c.expectError().getMessage shouldBe "BOOM TROIS!"
    latch.countDown()
  }

  "signal future failure asap" in {
    val latch = TestLatch(1)
    val done =
      Source(1 to 5)
        .map { elem =>
          if (elem == 1) elem
          else {
            // Slow the upstream after the first
            Await.ready(latch, 10.seconds)
            elem
          }
        }
        .mapAsyncPartitioned(4, 2)(_ % 3) { (elem, _) =>
          if (elem == 1) Future.failed(new TE("BOOM EIN!"))
          else Future.successful(elem)
        }
        .runWith(Sink.ignore)

    intercept[TE] {
      Await.result(done, remainingOrDefault)
    }.getMessage shouldBe "BOOM EIN!"
    latch.countDown()
  }

  "fail ASAP midstream" in {
    import scala.collection.immutable

    import system.dispatcher

    val promises = (0 until 6).map(_ => Promise[Int]()).toArray
    val probe =
      Source(0 until 6)
        .mapAsyncPartitioned(5, 1)(_ % 7) { (elem, _) =>
          promises(elem).future.map(n => ('A' + n).toChar)
        }
        .runWith(TestSink())

    probe.request(100)
    val failure = new Exception("BOOM två")
    scala.util.Random.shuffle((0 until 6): immutable.Seq[Int]).foreach { n =>
      if (n == 2) promises(n).failure(failure)
      else promises(n).success(n)
    }

    // we don't know when the third promise will be failed
    probe.expectNextOrError() match {
      case Left(ex) => ex.getMessage shouldBe failure.getMessage // fine, error can overtake elements
      case Right('A') =>
        probe.expectNextOrError() match {
          case Left(ex) => ex.getMessage shouldBe failure.getMessage // fine, error can overtake elements
          case Right('B') =>
            probe.expectNextOrError() match {
              case Left(ex) => ex.getMessage shouldBe failure.getMessage // fine, error can overtake elements
              case Right(n) => fail(s"stage should have failed rather than emit $n")
            }

          case unexpected => fail(s"unexpected $unexpected")
        }

      case unexpected => fail(s"unexpected $unexpected")
    }
  }

  "signal error when constructing future" in {
    import system.dispatcher

    val latch = TestLatch(1)
    val c = TestSubscriber.manualProbe[Int]()

    Source(1 to 5)
      .mapAsyncPartitioned(4, 2)(_ % 2) { (elem, _) =>
        if (elem == 3) throw new TE("BOOM TRE!")
        else
          Future {
            Await.ready(latch, 10.seconds)
            elem
          }
      }
      .to(Sink.fromSubscriber(c))
      .run()

    val sub = c.expectSubscription()
    sub.request(10)
    c.expectError().getMessage shouldBe "BOOM TRE!"
    latch.countDown()
  }

  "ignore null-completed futures" in {
    val shouldBeNull = {
      val n = scala.util.Random.nextInt(10) + 1
      (1 to n).foldLeft(Set.empty[Int]) { (set, _) =>
        set + scala.util.Random.nextInt(10)
      }
    }
    if (shouldBeNull.isEmpty) fail("should be at least one null")

    val f: (Int, Int) => Future[String] = { (elem, _) =>
      if (shouldBeNull(elem)) Future.successful(null)
      else Future.successful(elem.toString)
    }

    val result =
      Source(1 to 10).mapAsyncPartitioned(5, 2)(_ % 2)(f).runWith(Sink.seq)

    result.futureValue should contain theSameElementsInOrderAs (1 to 10).filterNot(shouldBeNull).map(_.toString)
  }

  "handle cancel properly" in {
    val pub = TestPublisher.manualProbe[Int]()
    val sub = TestSubscriber.manualProbe[Int]()

    Source
      .fromPublisher(pub)
      .mapAsyncPartitioned(4, 2)(_ % 2) { (elem, _) =>
        Future.successful(elem)
      }
      .runWith(Sink.fromSubscriber(sub))

    val upstream = pub.expectSubscription()
    upstream.expectRequest()

    sub.expectSubscription().cancel()

    upstream.expectCancellation()
  }

  val maxParallelism = Runtime.getRuntime.availableProcessors.max(8)
  val perPartition = scala.util.Random.nextInt(maxParallelism - 1) + 1
  val numPartitions = 1 + (maxParallelism / perPartition)

  s"not run more futures than allowed overall or per-partition ($maxParallelism, $perPartition, $numPartitions)" in {
    val partitioner: Int => Int = _ % (1 + (maxParallelism / perPartition))

    val globalCounter = new AtomicInteger
    val partitionCounters =
      (0 until numPartitions).map(_ -> new AtomicInteger).toMap: Map[Int, AtomicInteger]
    val queue = new LinkedBlockingQueue[(Int, Promise[Int], Long)]

    val timer = new Thread {
      val maxDelay = 100000 // nanos

      def delay(): Int =
        scala.util.Random.nextInt(maxDelay) + 1

      var count = 0

      @annotation.tailrec
      final override def run(): Unit = {
        val cont =
          try {
            val (partition, promise, enqueued) = queue.take()
            val wakeup = enqueued + delay()
            while (System.nanoTime() < wakeup) {}
            globalCounter.decrementAndGet()
            partitionCounters(partition).decrementAndGet()
            promise.success(count)
            count += 1
            true
          } catch {
            case _: InterruptedException => false
          }

        if (cont) run()
      }
    }
    timer.start()

    def deferred(partition: Int): Future[Int] =
      if (globalCounter.incrementAndGet() > maxParallelism) {
        Future.failed(new AssertionError("global parallelism exceeded"))
      } else if (partitionCounters(partition).incrementAndGet() > perPartition) {
        Future.failed(new AssertionError(s"partition parallelism for partition $partition exceeded"))
      } else {
        val p = Promise[Int]()
        queue.offer((partition, p, System.nanoTime()))
        p.future
      }

    try {
      @volatile var lastElem = 0
      val successes =
        Source(1 to 10000)
          .mapAsyncPartitioned(maxParallelism, perPartition)(partitioner) { (_, partition) =>
            deferred(partition)
          }
          .filter { elem =>
            if (elem == lastElem + 1) {
              lastElem = elem
              true
            } else false
          }
          .runFold(0) { (c, e) =>
            c + e
          }

      successes.futureValue shouldBe (5000 * 9999)
    } finally {
      timer.interrupt()
    }
  }

  val thisWillNotStand = TE("this will not stand")

  def deciderTest(f: (Boolean, Boolean) => Future[Boolean]): Assertion = {
    val failCount = new AtomicInteger
    val result =
      Source(List(true, false))
        .mapAsyncPartitioned(2, 1)(identity)(f)
        .addAttributes(ActorAttributes.supervisionStrategy {
          case x if x == thisWillNotStand =>
            failCount.incrementAndGet()
            Supervision.resume

          case _ => Supervision.stop
        })
        .runWith(Sink.seq)

    result.futureValue should contain only false
    failCount.get() shouldBe 1
  }

}
