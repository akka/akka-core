/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

object TakeWhile {
  def takeWhileExample(): Unit = {
    import akka.actor.ActorSystem
    import akka.stream.scaladsl.Source

    implicit val system: ActorSystem = ActorSystem()

    // #take-while
    Source(1 to 10).takeWhile(_ < 3).runForeach(println)
    // prints
    // 1
    // 2
    // #take-while
  }
}
