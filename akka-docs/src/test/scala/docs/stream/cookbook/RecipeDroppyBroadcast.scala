/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.stream.{ ClosedShape, OverflowStrategy }
import akka.stream.scaladsl._
import akka.stream.testkit._

class RecipeDroppyBroadcast extends RecipeSpec {

  "Recipe for a droppy broadcast" must {
    "work" in {
      val pub = TestPublisher.Probe[Int]()
      val myElements = Source.fromPublisher(pub)

      val sub1 = TestSubscriber.ManualProbe[Int]()
      val sub2 = TestSubscriber.ManualProbe[Int]()
      val sub3 = TestSubscriber.Probe[Int]()
      val mySink1 = Sink.fromSubscriber(sub1)
      val mySink2 = Sink.fromSubscriber(sub2)
      val mySink3 = Sink.fromSubscriber(sub3)

      //#droppy-bcast
      val graph = RunnableGraph.fromGraph(GraphDSL.createGraph(mySink1, mySink2, mySink3)((_, _, _)) {
        implicit b => (sink1, sink2, sink3) =>
          import GraphDSL.Implicits._

          val bcast = b.add(Broadcast[Int](3))
          myElements ~> bcast

          bcast.buffer(10, OverflowStrategy.dropHead) ~> sink1
          bcast.buffer(10, OverflowStrategy.dropHead) ~> sink2
          bcast.buffer(10, OverflowStrategy.dropHead) ~> sink3
          ClosedShape
      })
      //#droppy-bcast

      graph.run()

      sub3.request(100)
      for (i <- 1 to 100) {
        pub.sendNext(i)
        sub3.expectNext(i)
      }

      pub.sendComplete()

      sub1.expectSubscription().request(10)
      sub2.expectSubscription().request(10)

      for (i <- 91 to 100) {
        sub1.expectNext(i)
        sub2.expectNext(i)
      }

      sub1.expectComplete()
      sub2.expectComplete()

    }
  }

}
