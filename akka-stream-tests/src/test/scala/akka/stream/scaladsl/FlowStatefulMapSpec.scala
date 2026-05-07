/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.util.Success
import scala.util.control.NoStackTrace

import akka.Done
import akka.stream.AbruptStageTerminationException
import akka.stream.ActorAttributes
import akka.stream.ActorMaterializer
import akka.stream.Supervision
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.EventFilter

class FlowStatefulMapSpec extends StreamSpec {

  val ex = new Exception("TEST") with NoStackTrace

  "A StatefulMap" must {
    "work in the happy case" in {
      val sinkProb = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => 0)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, _ => None)
        .runWith(TestSink[(Int, Int)]())
      sinkProb.expectSubscription().request(6)
      sinkProb
        .expectNext((0, 1))
        .expectNext((1, 2))
        .expectNext((3, 3))
        .expectNext((6, 4))
        .expectNext((10, 5))
        .expectComplete()
    }

    "can remember the state when complete" in {
      val sinkProb = Source(1 to 10)
        .statefulMap(() => List.empty[Int])(
          (state, elem) => {
            //grouped 3 elements into a list
            val newState = elem :: state
            if (newState.size == 3)
              (Nil, newState.reverse)
            else
              (newState, Nil)
          },
          state => Some(state.reverse))
        .mapConcat(identity)
        .runWith(TestSink[Int]())
      sinkProb.expectSubscription().request(10)
      sinkProb.expectNextN(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).expectComplete()
    }

    "be able to resume" in {
      val testSink = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => 0)((agg, elem) => {
          if (elem % 2 == 0)
            throw ex
          else
            (agg + elem, (agg, elem))
        }, _ => None)
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
        .runWith(TestSink[(Int, Int)]())

      testSink.expectSubscription().request(5)
      testSink.expectNext((0, 1)).expectNext((1, 3)).expectNext((4, 5)).expectComplete()
    }

    "be able to restart" in {
      val testSink = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => 0)((agg, elem) => {
          if (elem % 3 == 0)
            throw ex
          else
            (agg + elem, (agg, elem))
        }, _ => None)
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(TestSink[(Int, Int)]())

      testSink.expectSubscription().request(5)
      testSink.expectNext((0, 1)).expectNext((1, 2)).expectNext((0, 4)).expectNext((4, 5)).expectComplete()
    }

    "be able to stop" in {
      val testSink = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => 0)((agg, elem) => {
          if (elem % 3 == 0)
            throw ex
          else
            (agg + elem, (agg, elem))
        }, _ => None)
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.stoppingDecider))
        .runWith(TestSink[(Int, Int)]())

      testSink.expectSubscription().request(5)
      testSink.expectNext((0, 1)).expectNext((1, 2)).expectError(ex)
    }

    "fail on upstream failure" in {
      val (testSource, testSink) = TestSource[Int]()
        .statefulMap(() => 0)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, _ => None)
        .toMat(TestSink[(Int, Int)]())(Keep.both)
        .run()

      testSink.expectSubscription().request(5)
      testSource.sendNext(1)
      testSink.expectNext((0, 1))
      testSource.sendNext(2)
      testSink.expectNext((1, 2))
      testSource.sendNext(3)
      testSink.expectNext((3, 3))
      testSource.sendNext(4)
      testSink.expectNext((6, 4))
      testSource.sendError(ex)
      testSink.expectError(ex)
    }

    "defer upstream failure and remember state" in {
      val (testSource, testSink) = TestSource[Int]()
        .statefulMap(() => 0)((agg, elem) => { (agg + elem, (agg, elem)) }, (state: Int) => Some((state, -1)))
        .toMat(TestSink[(Int, Int)]())(Keep.both)
        .run()

      testSink.expectSubscription().request(5)
      testSource.sendNext(1)
      testSink.expectNext((0, 1))
      testSource.sendNext(2)
      testSink.expectNext((1, 2))
      testSource.sendNext(3)
      testSink.expectNext((3, 3))
      testSource.sendNext(4)
      testSink.expectNext((6, 4))
      testSource.sendError(ex)
      testSink.expectNext((10, -1))
      testSink.expectError(ex)
    }

    "cancel upstream when downstream cancel" in {
      val promise = Promise[Done]()
      val testSource = TestSource[Int]()
        .statefulMap(() => 100)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, (state: Int) => {
          promise.complete(Success(Done))
          Some((state, -1))
        })
        .toMat(Sink.cancelled)(Keep.left)
        .run()
      testSource.expectSubscription().expectCancellation()
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "cancel upstream when downstream fail" in {
      val promise = Promise[Done]()
      val testProb = TestSubscriber.probe[(Int, Int)]()
      val testSource = TestSource[Int]()
        .statefulMap(() => 100)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, (state: Int) => {
          promise.complete(Success(Done))
          Some((state, -1))
        })
        .toMat(Sink.fromSubscriber(testProb))(Keep.left)
        .run()
      testProb.cancel(ex)
      testSource.expectCancellationWithCause(ex)
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "call its onComplete callback on abrupt materializer termination" in {
      @nowarn("msg=deprecated")
      val mat = ActorMaterializer()
      val promise = Promise[Done]()

      val matVal = Source
        .single(1)
        .statefulMap(() => -1)((_, elem) => (elem, elem), _ => {
          promise.complete(Success(Done))
          None
        })
        .runWith(Sink.never)(mat)
      mat.shutdown()
      matVal.failed.futureValue shouldBe an[AbruptStageTerminationException]
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "call its onComplete callback when stop" in {
      val promise = Promise[Done]()
      Source
        .single(1)
        .statefulMap(() => -1)((_, _) => throw ex, _ => {
          promise.complete(Success(Done))
          None
        })
        .runWith(Sink.ignore)
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "be able to be used as zipWithIndex" in {
      Source(List("A", "B", "C", "D"))
        .statefulMap(() => 0L)((index, elem) => (index + 1, (elem, index)), _ => None)
        .runWith(TestSink[(String, Long)]())
        .request(4)
        .expectNext(("A", 0L))
        .expectNext(("B", 1L))
        .expectNext(("C", 2L))
        .expectNext(("D", 3L))
        .expectComplete()
    }

    "be able to be used as bufferUntilChanged" in {
      val sink = TestSink[List[String]]()
      Source("A" :: "B" :: "B" :: "C" :: "C" :: "C" :: "D" :: Nil)
        .statefulMap(() => List.empty[String])(
          (buffer, elem) =>
            buffer match {
              case head :: _ if head != elem => (elem :: Nil, buffer)
              case _                         => (elem :: buffer, Nil)
            },
          buffer => Some(buffer))
        .filter(_.nonEmpty)
        .runWith(sink)
        .request(4)
        .expectNext(List("A"))
        .expectNext(List("B", "B"))
        .expectNext(List("C", "C", "C"))
        .expectNext(List("D"))
        .expectComplete()
    }

    "be able to be used as distinctUntilChanged" in {
      Source("A" :: "B" :: "B" :: "C" :: "C" :: "C" :: "D" :: Nil)
        .statefulMap(() => Option.empty[String])(
          (lastElement, elem) =>
            lastElement match {
              case Some(head) if head == elem => (Some(elem), None)
              case _                          => (Some(elem), Some(elem))
            },
          _ => None)
        .collect({ case Some(elem) => elem })
        .runWith(TestSink[String]())
        .request(4)
        .expectNext("A")
        .expectNext("B")
        .expectNext("C")
        .expectNext("D")
        .expectComplete()
    }

    "will not call onComplete twice if `f` fail" in {
      val closedCounter = new AtomicInteger(0)
      val probe = Source
        .repeat(1)
        .statefulMap(() => 23)( // the best resource there is
          (_, _) => throw TE("failing read"),
          _ => {
            closedCounter.incrementAndGet()
            None
          })
        .runWith(TestSink[Int]())

      probe.request(1)
      probe.expectError(TE("failing read"))
      closedCounter.get() should ===(1)
    }

    "will not call onComplete twice if `f` and `onComplete` both fail" in {
      val closedCounter = new AtomicInteger(0)
      val probe = Source
        .repeat(1)
        .statefulMap(() => 23)((_, _) => throw TE("failing read"), _ => {
          closedCounter.incrementAndGet()
          if (closedCounter.get == 1) {
            throw TE("boom")
          }
          None
        })
        .runWith(TestSink[Int]())

      EventFilter[TE](occurrences = 1).intercept {
        probe.request(1)
        probe.expectError(TE("boom"))
      }
      closedCounter.get() should ===(1)
    }

    "will not call onComplete twice on cancel when `onComplete` fails" in {
      val closedCounter = new AtomicInteger(0)
      val (source, sink) = TestSource()
        .viaMat(Flow[Int].statefulMap(() => 23)((s, elem) => (s, elem), _ => {
          closedCounter.incrementAndGet()
          throw TE("boom")
        }))(Keep.left)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      EventFilter[TE](occurrences = 1).intercept {
        sink.request(1)
        source.sendNext(1)
        sink.expectNext(1)
        sink.cancel()
        source.expectCancellation()
      }
      closedCounter.get() should ===(1)
    }

    "will not call onComplete twice if `onComplete` fail on upstream complete" in {
      val closedCounter = new AtomicInteger(0)
      val (pub, sub) = TestSource[Int]()
        .statefulMap(() => 23)((state, value) => (state, value), _ => {
          closedCounter.incrementAndGet()
          throw TE("boom")
        })
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      EventFilter[TE](occurrences = 1).intercept {
        sub.request(1)
        pub.sendNext(1)
        sub.expectNext(1)
        sub.request(1)
        pub.sendComplete()
        sub.expectError(TE("boom"))
      }

      closedCounter.get() should ===(1)
    }

    "emit onClose return value before restarting" in {
      val stateCounter = new AtomicInteger(0)
      val (source, sink) = TestSource[String]()
        .viaMat(Flow[String].statefulMap(() => stateCounter.incrementAndGet())({ (s, elem) =>
          if (elem == "boom") throw TE("boom")
          else (s, elem + s.toString)
        }, _ => Some("onClose")))(Keep.left)
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .toMat(TestSink())(Keep.both)
        .run()

      sink.request(1)
      source.sendNext("one")
      sink.expectNext("one1")
      sink.request(1)
      source.sendNext("boom")
      sink.expectNext("onClose")
      sink.request(1)
      source.sendNext("two")
      sink.expectNext("two2")
      sink.cancel()
      source.expectCancellation()
    }

    "not allow null state" in {
      EventFilter[NullPointerException](occurrences = 1).intercept {
        Source
          .single("one")
          .statefulMap(() => null: String)((s, t) => (s, t), _ => None)
          .runWith(Sink.head)
          .failed
          .futureValue shouldBe a[NullPointerException]
      }
    }

    "not allow null next state" in {
      EventFilter[NullPointerException](occurrences = 1).intercept {
        Source
          .single("one")
          .statefulMap(() => "state")((_, t) => (null, t), _ => None)
          .runWith(Sink.seq)
          .failed
          .futureValue shouldBe a[NullPointerException]
      }
    }

    "not allow null state on restart" in {
      val counter = new AtomicInteger(0)
      EventFilter[NullPointerException](occurrences = 1).intercept {
        Source
          .single("one")
          .statefulMap(() => if (counter.incrementAndGet() == 1) "state" else null)(
            (_, _) => throw TE("boom"),
            _ => None)
          .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
          .runWith(Sink.head)
          .failed
          .futureValue shouldBe a[NullPointerException]
      }
    }

  }
}
