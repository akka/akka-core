/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators

//#imports
import akka.NotUsed
import akka.stream.scaladsl._

//#imports

object Map {

  //#map
  val source: Source[Int, NotUsed] = Source(1 to 10)
  val mapped: Source[String, NotUsed] = source.map(elem => elem.toString)
  //#map
}
