/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.stream.scaladsl.Source
import akka.actor.ActorSystem

object Sliding {
  implicit val system: ActorSystem = ???

  def slidingExample1(): Unit = {
    //#sliding-1
    val source = Source(1 to 4)
    source.sliding(2).runForeach(println)
    // prints:
    // Vector(1, 2)
    // Vector(2, 3)
    // Vector(3, 4)
    //#sliding-1
  }

  def slidingExample2(): Unit = {
    //#sliding-2
    val source = Source(1 to 4)
    source.sliding(n = 3, step = 2).runForeach(println)
    // prints:
    // Vector(1, 2, 3)
    // Vector(3, 4) - shorter because stream ended before we got 3 elements
    //#sliding-2
  }

  def slidingExample3(): Unit = {
    //#moving-average
    val numbers = Source(1 :: 3 :: 10 :: 2 :: 3 :: 4 :: 2 :: 10 :: 11 :: Nil)
    val movingAverage = numbers.sliding(5).map(window => window.sum.toFloat / window.size)
    movingAverage.runForeach(println)
    // prints
    // 3.8 = average of 1, 3, 10, 2, 3
    // 4.4 = average of 3, 10, 2, 3, 4
    // 4.2 = average of 10, 2, 3, 4, 2
    // 4.2 = average of 2, 3, 4, 2, 10
    // 6.0 = average of 3, 4, 2, 10, 11
    //#moving-average
  }

}
