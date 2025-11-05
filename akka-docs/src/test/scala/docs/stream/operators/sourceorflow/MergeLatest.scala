/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

object MergeLatest extends App {
  implicit val system: ActorSystem = ActorSystem()

  //#mergeLatest
  val prices = Source(List(100, 101, 99, 103))
  val quantity = Source(List(1, 3, 4, 2))

  prices
    .mergeLatest(quantity)
    .map {
      case price :: quantity :: Nil => price * quantity
    }
    .runForeach(println)

  // prints something like:
  // 100
  // 101
  // 303
  // 297
  // 396
  // 412
  // 206
  //#mergeLatest
}
