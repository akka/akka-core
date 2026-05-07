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

class CollectionSinkSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) {

  "Sink.collection" when {
    "using Seq as Collection" must {
      "return a Seq[T] from a Source" in {
        val input = (1 to 6)
        val future: Future[immutable.Seq[Int]] = Source(input).runWith(Sink.collection)
        val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
        result should be(input.toSeq)
      }

      "return an empty Seq[T] from an empty Source" in {
        val input: immutable.Seq[Int] = Nil
        val future: Future[immutable.Seq[Int]] = Source.fromIterator(() => input.iterator).runWith(Sink.collection)
        val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
        result should be(input)
      }

      "fail the future on abrupt termination" in {
        val mat = Materializer(system)
        val probe = TestPublisher.probe()
        val future = Source.fromPublisher(probe).runWith(Sink.collection[Unit, Seq[Unit]])(mat)
        mat.shutdown()
        future.failed.futureValue shouldBe an[AbruptTerminationException]
      }
    }
    "using Vector as Collection" must {
      "return a Vector[T] from a Source" in {
        val input = (1 to 6)
        val future: Future[immutable.Vector[Int]] = Source(input).runWith(Sink.collection)
        val result: immutable.Vector[Int] = Await.result(future, remainingOrDefault)
        result should be(input.toVector)
      }

      "return an empty Vector[T] from an empty Source" in {
        val input = Nil
        val future: Future[immutable.Vector[Int]] =
          Source.fromIterator(() => input.iterator).runWith(Sink.collection[Int, Vector[Int]])
        val result: immutable.Vector[Int] = Await.result(future, remainingOrDefault)
        result should be(Vector.empty[Int])
      }
    }
  }
}
