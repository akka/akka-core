/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators

import akka.actor.ActorSystem

object PartitionDocExample {

  implicit val system: ActorSystem = ???

  //#partition
  import akka.NotUsed
  import akka.stream.Attributes
  import akka.stream.Attributes.LogLevels
  import akka.stream.ClosedShape
  import akka.stream.scaladsl.Flow
  import akka.stream.scaladsl.GraphDSL
  import akka.stream.scaladsl.Partition
  import akka.stream.scaladsl.RunnableGraph
  import akka.stream.scaladsl.Sink
  import akka.stream.scaladsl.Source

  val source: Source[Int, NotUsed] = Source(1 to 10)

  val even: Sink[Int, NotUsed] =
    Flow[Int].log("even").withAttributes(Attributes.logLevels(onElement = LogLevels.Info)).to(Sink.ignore)
  val odd: Sink[Int, NotUsed] =
    Flow[Int].log("odd").withAttributes(Attributes.logLevels(onElement = LogLevels.Info)).to(Sink.ignore)

  RunnableGraph
    .fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val partition = builder.add(Partition[Int](2, element => if (element % 2 == 0) 0 else 1))
      source ~> partition.in
      partition.out(0) ~> even
      partition.out(1) ~> odd
      ClosedShape
    })
    .run()

  //#partition
}
