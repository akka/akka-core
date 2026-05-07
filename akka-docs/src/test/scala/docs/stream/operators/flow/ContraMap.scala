/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.flow

//#imports
import akka.NotUsed
import akka.stream.scaladsl._

//#imports

object ContraMap {

  //#contramap
  val flow: Flow[Int, Int, NotUsed] = Flow[Int]
  val newFlow: Flow[String, Int, NotUsed] = flow.contramap(_.toInt)
  //#contramap
}
