/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Materializer, UniformFanInShape }
import akka.stream.testkit.{ StreamSpec, TestSubscriber }

import scala.annotation.nowarn

@nowarn // keep unused imports
class FlowZipWithIndexSpec extends StreamSpec {

//#zip-with-index
  import akka.stream.scaladsl.Source
  import akka.stream.scaladsl.Sink

//#zip-with-index
  val settings = ActorMaterializerSettings(system).withInputBuffer(initialSize = 2, maxSize = 16)

  implicit val materializer: Materializer = ActorMaterializer(settings)

  "A ZipWithIndex for Flow " must {

    "work in the happy case" in {
      val probe = TestSubscriber.manualProbe[(Int, Long)]()
      Source(7 to 10).zipWithIndex.runWith(Sink.fromSubscriber(probe))
      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext((7, 0L))
      probe.expectNext((8, 1L))

      subscription.request(1)
      probe.expectNext((9, 2L))
      subscription.request(1)
      probe.expectNext((10, 3L))

      probe.expectComplete()
    }

    "will works in GraphDSL" in {
      import akka.stream.ClosedShape
      val pickMaxOfThree = GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val zip1 = b.add(ZipWith[Int, Int, Int](math.max))
        val zip2 = b.add(ZipWith[Int, Int, Int](math.max))
        zip1.out ~> zip2.in0

        UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
      }

      val resultSink = TestSink[(Int, Long)]()

      val g = RunnableGraph.fromGraph(GraphDSL.createGraph(resultSink) { implicit b => sink =>
        import GraphDSL.Implicits._

        // importing the partial graph will return its shape (inlets & outlets)
        val pm3 = b.add(pickMaxOfThree)

        Source.single(1) ~> pm3.in(0)
        Source.single(2) ~> pm3.in(1)
        Source.single(3) ~> pm3.in(2)
        pm3.out.zipWithIndex ~> sink.in
        ClosedShape
      })
      val p = g.run()
      p.request(1)
      p.expectNext((3, 0L))
      p.expectComplete()
    }

    "work in fruit example" in {
      //#zip-with-index
      Source(List("apple", "orange", "banana")).zipWithIndex.runWith(Sink.foreach(println))
      // this will print ('apple', 0), ('orange', 1), ('banana', 2)
      //#zip-with-index
    }

  }
}
