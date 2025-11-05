/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

object Drop {

  implicit val system: ActorSystem = ActorSystem()

  def drop(): Unit = {
    // #drop
    val fiveInts: Source[Int, NotUsed] = Source(1 to 5)
    val droppedThreeInts: Source[Int, NotUsed] = fiveInts.drop(3)

    droppedThreeInts.runForeach(println)
    // 4
    // 5
    // #drop
  }

  def dropWhile(): Unit = {
    // #dropWhile
    val droppedWhileNegative = Source(-3 to 3).dropWhile(_ < 0)

    droppedWhileNegative.runForeach(println)
    // 0
    // 1
    // 2
    // 3
    // #dropWhile
  }

}
