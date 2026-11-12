/*
 * Copyright (C) 2014-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.NotUsed
import akka.pattern.FutureTimeoutSupport
import akka.stream.OverflowStrategy
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.switch
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt
import scala.util.control.NoStackTrace

class FlowFlatMapConcatParallelismSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest with FutureTimeoutSupport {
  val toSeq = Flow[Int].grouped(1000).toMat(Sink.head)(Keep.right)

  class BoomException extends RuntimeException("BOOM~~") with NoStackTrace
  "A flatMapConcat" must {

    for (i <- 1 until 129) {
      s"work with value presented sources with parallelism: $i" in {
        Source(
          List(
            Source.empty[Int],
            Source.single(1),
            Source.empty[Int],
            Source(List(2, 3, 4)),
            Source.future(Future.successful(5)),
            Source.lazyFuture(() => Future.successful(6)),
            Source.future(after(1.millis)(Future.successful(7)))))
          .flatMapConcat(i, identity)
          .runWith(toSeq)
          .futureValue should ===(1 to 7)
      }
    }

    def generateRandomValuePresentedSources(nums: Int): (Int, Seq[Source[Int, NotUsed]]) = {
      val seq = Seq.tabulate(nums) { _ =>
        val random = ThreadLocalRandom.current().nextInt(1, 10)
        (random: @switch) match {
          case 1 => Source.single(1)
          case 2 => Source(List(1))
          case 3 => Source.fromJavaStream(() => java.util.stream.Stream.of(1))
          case 4 => Source.future(Future.successful(1))
          case 5 => Source.future(after(1.millis)(Future.successful(1)))
          case _ => Source.empty[Int]
        }
      }
      val sum = seq.filterNot(_.eq(Source.empty[Int])).size
      (sum, seq)
    }

    def generateSequencedValuePresentedSources(nums: Int): (Int, Seq[Source[Int, NotUsed]]) = {
      val seq = Seq.tabulate(nums) { index =>
        val random = ThreadLocalRandom.current().nextInt(1, 6)
        (random: @switch) match {
          case 1 => Source.single(index)
          case 2 => Source(List(index))
          case 3 => Source.fromJavaStream(() => java.util.stream.Stream.of(index))
          case 4 => Source.future(Future.successful(index))
          case 5 => Source.future(after(1.millis)(Future.successful(index)))
          case _ => throw new IllegalStateException("unexpected")
        }
      }
      val sum = (0 until nums).sum
      (sum, seq)
    }

    for (i <- 1 until 129) {
      s"work with generated value presented sources with parallelism: $i " in {
        val (sum, sources) = generateRandomValuePresentedSources(100000)
        Source(sources)
          .flatMapConcat(i, identity)
          .runWith(Sink.seq)
          .map(_.sum)(ExecutionContext.parasitic)
          .futureValue shouldBe sum
      }
    }

    for (i <- 1 until 129) {
      s"work with generated value sequenced sources with parallelism: $i " in {
        val (sum, sources) = generateSequencedValuePresentedSources(100000)
        Source(sources)
          .flatMapConcat(i, identity)
          //check the order
          .statefulMap(() => -1)((pre, current) => {
            if (pre + 1 != current) {
              throw new IllegalStateException(s"expected $pre + 1 == $current")
            }
            (current, current)
          }, _ => None)
          .runWith(Sink.seq)
          .map(_.sum)(ExecutionContext.parasitic)
          .futureValue shouldBe sum
      }
    }

    "work with value presented failed sources" in {
      val ex = new BoomException
      Source(
        List(
          Source.empty[Int],
          Source.single(1),
          Source.empty[Int],
          Source(List(2, 3, 4)),
          Source.future(Future.failed(ex)),
          Source.lazyFuture(() => Future.successful(5))))
        .flatMapConcat(ThreadLocalRandom.current().nextInt(1, 129), identity)
        .onErrorComplete[BoomException]()
        .runWith(toSeq)
        .futureValue should ===(1 to 4)
    }

    "work with value presented sources when demands slow" in {
      val prob = Source(
        List(Source.empty[Int], Source.single(1), Source(List(2, 3, 4)), Source.lazyFuture(() => Future.successful(5))))
        .flatMapConcat(ThreadLocalRandom.current().nextInt(1, 129), identity)
        .runWith(TestSink())

      prob.request(1)
      prob.expectNext(1)
      prob.expectNoMessage(1.seconds)
      prob.request(2)
      prob.expectNext(2, 3)
      prob.expectNoMessage(1.seconds)
      prob.request(2)
      prob.expectNext(4, 5)
      prob.expectComplete()
    }

    "can do pre materialization when parallelism > 1" in {
      val materializationCounter = new AtomicInteger(0)
      val randomParallelism = ThreadLocalRandom.current().nextInt(4, 65)
      val prob = Source(1 to (randomParallelism * 3))
        .flatMapConcat(
          randomParallelism,
          value => {
            Source
              .lazySingle(() => {
                materializationCounter.incrementAndGet()
                value
              })
              .buffer(1, overflowStrategy = OverflowStrategy.backpressure)
          })
        .runWith(TestSink())

      expectNoMessage(1.seconds)
      materializationCounter.get() shouldBe 0

      prob.request(1)
      prob.expectNext(1.seconds, 1)
      expectNoMessage(1.seconds)
      materializationCounter.get() shouldBe (randomParallelism + 1)
      materializationCounter.set(0)

      prob.request(2)
      prob.expectNextN(List(2, 3))
      expectNoMessage(1.seconds)
      materializationCounter.get() shouldBe 2
      materializationCounter.set(0)

      prob.request(randomParallelism - 3)
      prob.expectNextN(4 to randomParallelism)
      expectNoMessage(1.seconds)
      materializationCounter.get() shouldBe (randomParallelism - 3)
      materializationCounter.set(0)

      prob.request(randomParallelism)
      prob.expectNextN(randomParallelism + 1 to randomParallelism * 2)
      expectNoMessage(1.seconds)
      materializationCounter.get() shouldBe randomParallelism
      materializationCounter.set(0)

      prob.request(randomParallelism)
      prob.expectNextN(randomParallelism * 2 + 1 to randomParallelism * 3)
      expectNoMessage(1.seconds)
      materializationCounter.get() shouldBe 0
      prob.expectComplete()
    }

  }

}
