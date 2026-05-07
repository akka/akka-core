/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.testkit.{ ScriptedTest, StreamSpec, TestPublisher, TestSubscriber }
import akka.testkit.TimingTest

class FlowGroupedWeightedSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest {

  "A GroupedWeighted" must {
    "produce no group (empty sink sequence) when source is empty" in {
      val input = immutable.Seq.empty
      def costFn(e: Int): Long = 999999L // set to an arbitrarily big value
      val future = Source(input).groupedWeighted(1)(costFn).runWith(Sink.seq)
      val result = Await.result(future, remainingOrDefault)
      result should be(Seq.empty)
    }

    "always exhaust a source into a single group if cost is 0" in {
      val input = (1 to 15)
      def costFn(e: Int): Long = 0L
      val minWeight = 1 // chose the least possible value for minWeight
      val future = Source(input).groupedWeighted(minWeight)(costFn).runWith(Sink.seq)
      val result = Await.result(future, remainingOrDefault)
      result should be(Seq(input))
    }

    "exhaust source into one group if minWeight equals the accumulated cost of the source" in {
      val input = (1 to 16)
      def costFn(e: Int): Long = 1L
      val minWeight = input.length
      val future = Source(input).groupedWeighted(minWeight)(costFn).runWith(Sink.seq)
      val result = Await.result(future, remainingOrDefault)
      result should be(Seq(input))
    }

    "exhaust a source into one group if minWeight is greater than the accumulated cost of source" in {
      val input = List("this", "is", "some", "string")
      def costFn(e: String): Long = e.length
      val minWeight = Long.MaxValue
      val future = Source(input).groupedWeighted(minWeight)(costFn).runWith(Sink.seq)
      val result = Await.result(future, remainingOrDefault)
      result should be(Seq(input))
    }

    "emit a group each time the grouped weight is greater than minWeight" in {
      val input = List(1, 2, 1, 2)
      def costFn(e: Int): Long = e
      val minWeight = 2 // must produce two groups of List(1, 2)
      val future = Source(input).groupedWeighted(minWeight)(costFn).runWith(Sink.seq)
      val result: Seq[Seq[Int]] = Await.result(future, remainingOrDefault)
      result should be(Seq(Seq(1, 2), Seq(1, 2)))
    }

    "not emit group when grouped weight is less than minWeight and upstream has not completed" taggedAs TimingTest in {
      val p = TestPublisher.probe[Int]()
      val c = TestSubscriber.probe[immutable.Seq[Int]]()
      // Note that the cost function set to zero here means the stream will accumulate elements until completed
      Source.fromPublisher(p).groupedWeighted(10)(_ => 0L).to(Sink.fromSubscriber(c)).run()
      p.sendNext(1)
      c.expectSubscription().request(1) // create downstream demand so Grouped pulls on upstream
      c.expectNoMessage(50.millis) // message should not be emitted yet
      p.sendComplete() // Force Grouped to emit the small group
      c.expectNext(50.millis, immutable.Seq(1))
      c.expectComplete()
    }

    "fail during stream initialization when minWeight is negative" in {
      val ex = the[IllegalArgumentException] thrownBy Source(1 to 5)
          .groupedWeighted(-1)(_ => 1L)
          .to(Sink.collection)
          .run()
      ex.getMessage should be("requirement failed: minWeight must be greater than 0")
    }

    "fail during stream initialization when minWeight is 0" in {
      val ex = the[IllegalArgumentException] thrownBy Source(1 to 5)
          .groupedWeighted(0)(_ => 1L)
          .to(Sink.collection)
          .run()
      ex.getMessage should be("requirement failed: minWeight must be greater than 0")
    }

    "fail the stage when costFn has a negative result" in {
      val p = TestPublisher.probe[Int]()
      val c = TestSubscriber.probe[immutable.Seq[Int]]()
      Source.fromPublisher(p).groupedWeighted(1)(_ => -1L).to(Sink.fromSubscriber(c)).run()
      c.expectSubscription().request(1) // create downstream demand so Grouped pulls on upstream
      c.expectNoMessage(50.millis) // shouldn't fail until the message is sent
      p.sendNext(1) // Send a message that will result in negative cost
      val error = c.expectError()
      error shouldBe an[IllegalArgumentException]
      error.getMessage should be("Negative weight [-1] for element [1] is not allowed")
    }
  }
}
