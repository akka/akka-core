/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.StringJoiner

import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._
import akka.stream.testkit.scaladsl.TestSink

class FlowInterleaveAllSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  "An InterleaveAll for Flow " must {

    "work in the happy case" in assertAllStagesStopped {
      val sub = Source(List(1, 2, 7))
        .interleaveAll(List(Source(List(3, 4, 8)), Source(List(5, 6, 9, 10))), 2, eagerClose = false)
        .runWith(TestSink[Int]())
      sub.expectSubscription().request(10)
      sub.expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).expectComplete()
    }

    "work with none other sources" in {
      val sub = Source(List(1, 2, 3)).interleaveAll(Nil, 2, eagerClose = false).runWith(TestSink[Int]())
      sub.expectSubscription().request(3)
      sub.expectNext(1, 2, 3).expectComplete()
    }

    "work with empty other source" in {
      val sub =
        Source(List(1, 2, 3)).interleaveAll(List(Source.empty), 2, eagerClose = false).runWith(TestSink[Int]())
      sub.expectSubscription().request(3)
      sub.expectNext(1, 2, 3).expectComplete()
    }

    "eagerClose = true, first stream closed" in assertAllStagesStopped {
      val sub = Source(List(1, 2, 7))
        .interleaveAll(List(Source(List(3, 4, 8)), Source(List(5, 6, 9, 10))), 2, eagerClose = true)
        .runWith(TestSink[Int]())
      sub.expectSubscription().request(10)
      sub.expectNext(1, 2, 3, 4, 5, 6, 7).expectComplete()
    }

    "eagerClose = true, other stream closed" in assertAllStagesStopped {
      val probe = TestSubscriber.manualProbe[Int]()

      val pub1 = TestPublisher.probe[Int]()
      val pub2 = TestPublisher.probe[Int]()
      val pub3 = TestPublisher.probe[Int]()

      Source
        .fromPublisher(pub1)
        .interleaveAll(List(Source.fromPublisher(pub2), Source.fromPublisher(pub3)), 2, eagerClose = true)
        .runWith(Sink.fromSubscriber(probe))

      probe.expectSubscription().request(10)

      // just to make it extra clear that it eagerly pulls all inputs
      pub1.expectRequest()
      pub2.expectRequest()
      pub3.expectRequest()

      pub1.sendNext(0)
      pub2.sendNext(10)
      pub3.sendNext(20)

      pub1.expectRequest()
      pub1.sendNext(1)

      pub2.expectRequest()
      pub2.sendNext(11)
      pub2.sendComplete()

      probe.expectNext(0, 1, 10, 11, 20)
      probe.expectComplete()

      pub1.expectCancellation()
      pub3.expectCancellation()
    }

    "pass along early cancellation" in assertAllStagesStopped {
      val pub1 = TestPublisher.manualProbe[Int]()
      val pub2 = TestPublisher.manualProbe[Int]()
      val pub3 = TestPublisher.manualProbe[Int]()
      val sub1 = TestSubscriber.manualProbe[Int]()

      Source
        .fromPublisher(pub1)
        .interleaveAll(List(Source.fromPublisher(pub2), Source.fromPublisher(pub3)), 2, eagerClose = false)
        .runWith(Sink.fromSubscriber(sub1))
      sub1.expectSubscription().cancel()
      pub1.expectSubscription().expectCancellation()
      pub2.expectSubscription().expectCancellation()
      pub3.expectSubscription().expectCancellation()
    }

    "work in example" in {
      //#interleaveAll
      val sourceA = Source(List(1, 2, 7, 8))
      val sourceB = Source(List(3, 4, 9))
      val sourceC = Source(List(5, 6))

      sourceA
        .interleaveAll(List(sourceB, sourceC), 2, eagerClose = false)
        .fold(new StringJoiner(","))((joiner, input) => joiner.add(String.valueOf(input)))
        .runWith(Sink.foreach(println))
      //prints 1,2,3,4,5,6,7,8,9
      //#interleaveAll
    }
  }

}
