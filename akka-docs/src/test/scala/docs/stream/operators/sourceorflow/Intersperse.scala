/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

object Intersperse extends App {
  import akka.actor.ActorSystem

  implicit val system: ActorSystem = ActorSystem()

  //#intersperse
  Source(1 to 4).map(_.toString).intersperse("[", ", ", "]").runWith(Sink.foreach(print))
  // prints
  // [1, 2, 3, 4]
  //#intersperse

  system.terminate()
}
