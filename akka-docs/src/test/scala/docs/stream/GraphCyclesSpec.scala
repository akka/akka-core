/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.stream.{ ClosedShape, OverflowStrategy }
import akka.stream.scaladsl._
import akka.testkit.AkkaSpec

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
class GraphCyclesSpec extends AkkaSpec {

  "Cycle demonstration" must {
    val source = Source.fromIterator(() => Iterator.from(0))

    "include a deadlocked cycle" in {

      // format: OFF
      //#deadlocked
      // WARNING! The graph below deadlocks!
      RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val merge = b.add(Merge[Int](2))
        val bcast = b.add(Broadcast[Int](2))

        source ~> merge ~> Flow[Int].map { s => println(s); s } ~> bcast ~> Sink.ignore
                  merge                    <~                      bcast
        ClosedShape
      })
      //#deadlocked
      // format: ON
    }

    "include an unfair cycle" in {
      // format: OFF
      //#unfair
      // WARNING! The graph below stops consuming from "source" after a few steps
      RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val merge = b.add(MergePreferred[Int](1))
        val bcast = b.add(Broadcast[Int](2))

        source ~> merge ~> Flow[Int].map { s => println(s); s } ~> bcast ~> Sink.ignore
                  merge.preferred              <~                  bcast
        ClosedShape
      })
      //#unfair
      // format: ON
    }

    "include a dropping cycle" in {
      // format: OFF
      //#dropping
      RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val merge = b.add(Merge[Int](2))
        val bcast = b.add(Broadcast[Int](2))

        source ~> merge ~> Flow[Int].map { s => println(s); s } ~> bcast ~> Sink.ignore
            merge <~ Flow[Int].buffer(10, OverflowStrategy.dropHead) <~ bcast
        ClosedShape
      })
      //#dropping
      // format: ON
    }

    "include a dead zipping cycle" in {
      // format: OFF
      //#zipping-dead
      // WARNING! The graph below never processes any elements
      RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val zip = b.add(ZipWith[Int, Int, Int]((left, right) => right))
        val bcast = b.add(Broadcast[Int](2))

        source ~> zip.in0
        zip.out.map { s => println(s); s } ~> bcast ~> Sink.ignore
        zip.in1             <~                bcast
        ClosedShape
      })
      //#zipping-dead
      // format: ON
    }

    "include a live zipping cycle" in {
      // format: OFF
      //#zipping-live
      RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val zip = b.add(ZipWith((left: Int, right: Int) => left))
        val bcast = b.add(Broadcast[Int](2))
        val concat = b.add(Concat[Int]())
        val start = Source.single(0)

        source ~> zip.in0
        zip.out.map { s => println(s); s } ~> bcast ~> Sink.ignore
        zip.in1 <~ concat <~ start
                   concat         <~          bcast
        ClosedShape
      })
      //#zipping-live
      // format: ON
    }

  }

}
