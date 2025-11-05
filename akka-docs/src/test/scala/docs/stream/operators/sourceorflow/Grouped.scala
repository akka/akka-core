/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow
import akka.stream.scaladsl.Source

object Grouped {
  def groupedExample(): Unit = {
    import akka.actor.ActorSystem

    implicit val system: ActorSystem = ActorSystem()

    //#grouped
    Source(1 to 7).grouped(3).runForeach(println)
    // Vector(1, 2, 3)
    // Vector(4, 5, 6)
    // Vector(7)

    Source(1 to 7).grouped(3).map(_.sum).runForeach(println)
    // 6   (= 1 + 2 + 3)
    // 15  (= 4 + 5 + 6)
    // 7   (= 7)
    //#grouped
  }

}
