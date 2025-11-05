/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext

object MapConcat {

  def mapConcat(): Unit = {
    import akka.actor.ActorSystem

    implicit val system: ActorSystem = ActorSystem()
    implicit val ec: ExecutionContext = system.dispatcher

    //#map-concat
    def duplicate(i: Int): List[Int] = List(i, i)

    Source(1 to 3).mapConcat(i => duplicate(i)).runForeach(println)
    // prints:
    // 1
    // 1
    // 2
    // 2
    // 3
    // 3
    //#map-concat

  }

}
