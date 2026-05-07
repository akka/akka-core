/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.NotUsed
import akka.stream.{ ClosedShape, Inlet, Outlet }
import akka.stream.testkit.{ TestSubscriber, TwoStreamsSetup }
import akka.stream.testkit.TestSubscriber.ManualProbe

class GraphMergePrioritizedSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = Int

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val mergePrioritized = b.add(MergePrioritized[Outputs](Seq(2, 8)))

    override def left: Inlet[Outputs] = mergePrioritized.in(0)
    override def right: Inlet[Outputs] = mergePrioritized.in(1)
    override def out: Outlet[Outputs] = mergePrioritized.out
  }

  "merge prioritized" must {
    commonTests()

    "stream data from all sources" in {
      val source1 = Source.fromIterator(() => (1 to 3).iterator)
      val source2 = Source.fromIterator(() => (4 to 6).iterator)
      val source3 = Source.fromIterator(() => (7 to 9).iterator)

      val priorities = Seq(6, 3, 1)

      val probe = TestSubscriber.manualProbe[Int]()

      threeSourceMerge(source1, source2, source3, priorities, probe).run()

      val subscription = probe.expectSubscription()

      var collected = Seq.empty[Int]
      for (_ <- 1 to 9) {
        subscription.request(1)
        collected :+= probe.expectNext()
      }

      collected.toSet should be(Set(1, 2, 3, 4, 5, 6, 7, 8, 9))
      probe.expectComplete()
    }

    "stream data with priority" in {
      val elementCount = 20000
      val source1 = Source.fromIterator(() => Iterator.continually(1).take(elementCount))
      val source2 = Source.fromIterator(() => Iterator.continually(2).take(elementCount))
      val source3 = Source.fromIterator(() => Iterator.continually(3).take(elementCount))

      val priorities = Seq(6, 3, 1)

      val probe = TestSubscriber.manualProbe[Int]()

      threeSourceMerge(source1, source2, source3, priorities, probe).run()

      val subscription = probe.expectSubscription()

      val builder = Seq.newBuilder[Int]
      for (_ <- 1 to elementCount) {
        subscription.request(1)
        builder += probe.expectNext()
      }
      val collected = builder.result()

      val ones = collected.count(_ == 1).toDouble
      val twos = collected.count(_ == 2).toDouble
      val threes = collected.count(_ == 3).toDouble

      (ones / twos) should ===(2d +- 1)
      (ones / threes) should ===(6d +- 1)
      (twos / threes) should ===(3d +- 1)
    }

    "stream data when only one source produces" in {
      val elementCount = 10
      val source1 = Source.fromIterator(() => Iterator.continually(1).take(elementCount))
      val source2 = Source.fromIterator[Int](() => Iterator.empty)
      val source3 = Source.fromIterator[Int](() => Iterator.empty)

      val priorities = Seq(6, 3, 1)

      val probe = TestSubscriber.manualProbe[Int]()

      threeSourceMerge(source1, source2, source3, priorities, probe).run()

      val subscription = probe.expectSubscription()

      var collected = Seq.empty[Int]
      for (_ <- 1 to elementCount) {
        subscription.request(1)
        collected :+= probe.expectNext()
      }

      val ones = collected.count(_ == 1)
      val twos = collected.count(_ == 2)
      val threes = collected.count(_ == 3)

      ones shouldEqual elementCount
      twos shouldEqual 0
      threes shouldEqual 0
    }

    "stream data with priority when only two sources produce" in {
      val elementCount = 20000
      val source1 = Source.fromIterator(() => Iterator.continually(1).take(elementCount))
      val source2 = Source.fromIterator(() => Iterator.continually(2).take(elementCount))
      val source3 = Source.fromIterator[Int](() => Iterator.empty)

      val priorities = Seq(6, 3, 1)

      val probe = TestSubscriber.manualProbe[Int]()

      threeSourceMerge(source1, source2, source3, priorities, probe).run()

      val subscription = probe.expectSubscription()

      val builder = Vector.newBuilder[Int]
      for (_ <- 1 to elementCount) {
        subscription.request(1)
        builder += probe.expectNext()
      }
      val collected = builder.result()

      val ones = collected.count(_ == 1).toDouble
      val twos = collected.count(_ == 2).toDouble
      val threes = collected.count(_ == 3)

      threes shouldEqual 0
      (ones / twos) should ===(2d +- 1)
    }
  }

  private def threeSourceMerge[T](
      source1: Source[T, NotUsed],
      source2: Source[T, NotUsed],
      source3: Source[T, NotUsed],
      priorities: Seq[Int],
      probe: ManualProbe[T]) = {
    RunnableGraph.fromGraph(GraphDSL.createGraph(source1, source2, source3)((_, _, _)) { implicit b => (s1, s2, s3) =>
      val merge = b.add(MergePrioritized[T](priorities))
      // introduce a delay on the consuming side making it more likely that
      // the actual prioritization happens and elements does not just pass through
      val delayFirst = b.add(Flow[T].initialDelay(50.millis))
      s1.out ~> merge.in(0)
      s2.out ~> merge.in(1)
      s3.out ~> merge.in(2)
      merge.out ~> delayFirst ~> Sink.fromSubscriber(probe)
      ClosedShape
    })
  }
}
