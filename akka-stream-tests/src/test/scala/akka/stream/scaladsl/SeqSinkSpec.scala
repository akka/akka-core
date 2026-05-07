/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.Future

import akka.stream.AbruptTerminationException
import akka.stream.Materializer
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher

class SeqSinkSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) {

  "Sink.toSeq" must {
    "return a Seq[T] from a Source" in {
      val input = (1 to 6)
      val future: Future[immutable.Seq[Int]] = Source(input).runWith(Sink.seq)
      val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
      result should be(input.toSeq)
    }

    "return an empty Seq[T] from an empty Source" in {
      val input: immutable.Seq[Int] = Nil
      val future: Future[immutable.Seq[Int]] = Source.fromIterator(() => input.iterator).runWith(Sink.seq)
      val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
      result should be(input)
    }

    "fail the future on abrupt termination" in {
      val mat = Materializer(system)
      val probe = TestPublisher.probe()
      val future: Future[immutable.Seq[Int]] =
        Source.fromPublisher(probe).runWith(Sink.seq)(mat)
      mat.shutdown()
      future.failed.futureValue shouldBe an[AbruptTerminationException]
    }
  }
}
