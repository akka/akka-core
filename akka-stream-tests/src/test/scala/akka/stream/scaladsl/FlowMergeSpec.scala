/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import org.reactivestreams.Publisher

import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._

class FlowMergeSpec extends BaseTwoStreamsSetup {

  override type Outputs = Int

  override def setup(p1: Publisher[Int], p2: Publisher[Int]) = {
    val subscriber = TestSubscriber.probe[Outputs]()
    Source.fromPublisher(p1).merge(Source.fromPublisher(p2)).runWith(Sink.fromSubscriber(subscriber))
    subscriber
  }

  "A Merge for Flow " must {

    "work in the happy case" in assertAllStagesStopped {
      // Different input sizes (4 and 6)
      val source1 = Source(0 to 3)
      val source2 = Source(List[Int]())
      val source3 = Source(4 to 9)
      val probe = TestSubscriber.manualProbe[Int]()

      source1.merge(source2).merge(source3).map(_ * 2).map(_ / 2).map(_ + 1).runWith(Sink.fromSubscriber(probe))

      val subscription = probe.expectSubscription()

      var collected = Set.empty[Int]
      for (_ <- 1 to 10) {
        subscription.request(1)
        collected += probe.expectNext()
      }

      collected should be(Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      probe.expectComplete()
    }

    commonTests()

    "work with one immediately completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(completedPublisher, nonemptyPublisher(1 to 4))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(4)
      (1 to 4).foreach(subscriber1.expectNext)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), completedPublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(4)
      (1 to 4).foreach(subscriber2.expectNext)
      subscriber2.expectComplete()
    }

    "work with one delayed completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(soonToCompletePublisher, nonemptyPublisher(1 to 4))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(4)
      (1 to 4).foreach(subscriber1.expectNext)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), soonToCompletePublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(4)
      (1 to 4).foreach(subscriber2.expectNext)
      subscriber2.expectComplete()
    }

    "work with one immediately failed and one nonempty publisher" in {
      // This is nondeterministic, multiple scenarios can happen
      pending
    }

    "work with one delayed failed and one nonempty publisher" in {
      // This is nondeterministic, multiple scenarios can happen
      pending
    }

    "pass along early cancellation" in assertAllStagesStopped {
      val up1 = TestPublisher.manualProbe[Int]()
      val up2 = TestPublisher.manualProbe[Int]()
      val down = TestSubscriber.manualProbe[Int]()

      val (graphSubscriber1, graphSubscriber2) = Source
        .asSubscriber[Int]
        .mergeMat(Source.asSubscriber[Int])((_, _))
        .toMat(Sink.fromSubscriber(down))(Keep.left)
        .run()

      val downstream = down.expectSubscription()
      downstream.cancel()
      up1.subscribe(graphSubscriber1)
      up2.subscribe(graphSubscriber2)
      up1.expectSubscription().expectCancellation()
      up2.expectSubscription().expectCancellation()
    }

    "not try to grab from a closed input previously enqueued" in assertAllStagesStopped {
      // Coverage for #21138
      val up1 = TestPublisher.probe[Int]()
      val up2 = TestPublisher.probe[Int]()
      val down = TestSubscriber.probe[Int]()

      Source
        .fromPublisher(up1)
        .merge(Source.fromPublisher(up2), eagerComplete = true)
        .to(Sink.fromSubscriber(down))
        .run()

      up1.ensureSubscription()
      up2.ensureSubscription()
      down.ensureSubscription()

      up1.expectRequest()
      up2.expectRequest()
      up1.sendNext(7)
      up2.sendNext(8)
      // there is a race here, the 8 needs to be queued before the
      // source completes (it failed consistently on my machine, before bugfix)
      up2.sendComplete()
      down.request(1)
      down.expectNext()

    }

    "works in number example for mergePreferred" in {
      //#mergePreferred
      import akka.stream.scaladsl.{ Sink, Source }

      val sourceA = Source(List(1, 2, 3, 4))
      val sourceB = Source(List(10, 20, 30, 40))

      sourceA.mergePreferred(sourceB, false).runWith(Sink.foreach(println))
      // prints 1, 10, ... since both sources have their first element ready and the left source is preferred

      sourceA.mergePreferred(sourceB, true).runWith(Sink.foreach(println))
      // prints 10, 1, ... since both sources have their first element ready and the right source is preferred
      //#mergePreferred
    }

    "works in number example for mergePrioritized" in {
      //#mergePrioritized
      import akka.stream.scaladsl.{ Sink, Source }

      val sourceA = Source(List(1, 2, 3, 4))
      val sourceB = Source(List(10, 20, 30, 40))

      sourceA.mergePrioritized(sourceB, 99, 1).runWith(Sink.foreach(println))
      // prints e.g. 1, 10, 2, 3, 4, 20, 30, 40 since both sources have their first element ready and the left source
      // has higher priority – if both sources have elements ready, sourceA has a 99% chance of being picked next
      // while sourceB has a 1% chance
      //#mergePrioritized
    }

    "works in number example for mergePrioritizedN" in {
      //#mergePrioritizedN
      import akka.stream.scaladsl.{ Sink, Source }

      val sourceA = Source(List(1, 2, 3, 4))
      val sourceB = Source(List(10, 20, 30, 40))
      val sourceC = Source(List(100, 200, 300, 400))

      Source
        .mergePrioritizedN(List((sourceA, 9900), (sourceB, 99), (sourceC, 1)), eagerComplete = false)
        .runWith(Sink.foreach(println))
      // prints e.g. 1, 100, 2, 3, 4, 10, 20, 30, 40, 200, 300, 400  since both sources have their first element ready and
      // the left sourceA has higher priority - if both sources have elements ready, sourceA has a 99% chance of being picked next
      // while sourceB has a 0.99% chance and sourceC has a 0.01% chance
      //#mergePrioritizedN
    }

    "works in number example for merge sorted" in {
      //#merge-sorted
      import akka.stream.scaladsl.{ Sink, Source }

      val sourceA = Source(List(1, 3, 5, 7))
      val sourceB = Source(List(2, 4, 6, 8))

      sourceA.mergeSorted(sourceB).runWith(Sink.foreach(println))
      //prints 1, 2, 3, 4, 5, 6, 7, 8

      val sourceC = Source(List(20, 1, 1, 1))

      sourceA.mergeSorted(sourceC).runWith(Sink.foreach(println))
      //prints 1, 3, 5, 7, 20, 1, 1, 1
      //#merge-sorted
    }

    "works in number example for merge" in {
      //#merge
      import akka.stream.scaladsl.{ Sink, Source }

      val sourceA = Source(List(1, 2, 3, 4))
      val sourceB = Source(List(10, 20, 30, 40))

      sourceA.merge(sourceB).runWith(Sink.foreach(println))
      // merging is not deterministic, can for example print 1, 2, 3, 4, 10, 20, 30, 40
      //#merge
    }
  }
}
