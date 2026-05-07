/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Flow

object GroupBy {

  def groupBySourceExample(): Unit = {
    implicit val system: ActorSystem = ???
    //#groupBy
    Source(1 to 10)
      .groupBy(maxSubstreams = 2, _ % 2) // create two sub-streams with odd and even numbers
      .reduce(_ + _) // for each sub-stream, sum its elements
      .mergeSubstreams // merge back into a stream
      .runForeach(println)
    //30
    //25
    //#groupBy
  }

  def groupBySourceWithAsyncExample(): Unit = {
    implicit val system: ActorSystem = ???
    //#groupByWithAsync
    Source(1 to 10)
      .groupBy(maxSubstreams = 2, _ % 2) // create two sub-streams with odd and even numbers
      .via(Flow[Int].map(_ => 1).reduce(_ + _).async) // for each sub-stream, sum its elements
      .mergeSubstreams // merge back into a stream
      .runForeach(println)
    //30
    //25
    //#groupByWithAsync
  }

}
