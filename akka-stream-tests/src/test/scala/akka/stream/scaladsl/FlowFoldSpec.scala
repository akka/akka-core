/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.Supervision
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils._

class FlowFoldSpec extends StreamSpec {

  "A Fold" must {
    val input = 1 to 100
    val expected = input.sum
    val inputSource = Source(input)
    val foldSource = inputSource.fold[Int](0)(_ + _)
    val foldFlow = Flow[Int].fold(0)(_ + _)
    val foldSink = Sink.fold[Int, Int](0)(_ + _)

    "work when using Source.runFold" in {
      Await.result(inputSource.runFold(0)(_ + _), 3.seconds) should be(expected)
    }

    "work when using Source.fold" in {
      Await.result(foldSource.runWith(Sink.head), 3.seconds) should be(expected)
    }

    "work when using Sink.fold" in {
      Await.result(inputSource.runWith(foldSink), 3.seconds) should be(expected)
    }

    "work when using Flow.fold" in {
      Await.result(inputSource.via(foldFlow).runWith(Sink.head), 3.seconds) should be(expected)
    }

    "work when using Source.fold + Flow.fold + Sink.fold" in {
      Await.result(foldSource.via(foldFlow).runWith(foldSink), 3.seconds) should be(expected)
    }

    "propagate an error" in {
      val error = TE("Boom!")
      val future = inputSource.map(x => if (x > 50) throw error else x).runFold[NotUsed](NotUsed)(Keep.none)
      the[Exception] thrownBy Await.result(future, 3.seconds) should be(error)
    }

    "complete future with failure when the folding function throws and the supervisor strategy decides to stop" in {
      val error = TE("Boom!")
      val future = inputSource.runFold(0)((x, y) => if (x > 50) throw error else x + y)
      the[Exception] thrownBy Await.result(future, 3.seconds) should be(error)
    }

    "resume with the accumulated state when the folding function throws and the supervisor strategy decides to resume" in {
      val error = TE("Boom!")
      val fold = Sink.fold[Int, Int](0)((x, y) => if (y == 50) throw error else x + y)
      val future =
        inputSource.runWith(fold.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider)))

      Await.result(future, 3.seconds) should be(expected - 50)
    }

    "resume and reset the state when the folding function throws when the supervisor strategy decides to restart" in {
      val error = TE("Boom!")
      val fold = Sink.fold[Int, Int](0)((x, y) => if (y == 50) throw error else x + y)
      val future =
        inputSource.runWith(fold.withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider)))

      Await.result(future, 3.seconds) should be((51 to 100).sum)
    }

    "complete future and return zero given an empty stream" in {
      val futureValue =
        Source.fromIterator[Int](() => Iterator.empty).runFold(0)(_ + _)

      Await.result(futureValue, 3.seconds) should be(0)
    }

  }

}
