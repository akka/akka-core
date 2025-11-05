/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

//#imports
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.{ ExecutionContext, Future }
//#imports

object FoldAsync extends App {

  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher

  //#foldAsync
  case class Histogram(low: Long = 0, high: Long = 0) {
    def add(i: Int): Future[Histogram] =
      if (i < 100) Future { copy(low = low + 1) } else Future { copy(high = high + 1) }
  }

  Source(1 to 150).foldAsync(Histogram())((acc, n) => acc.add(n)).runForeach(println)

  // Prints: Histogram(99,51)
  //#foldAsync
}
