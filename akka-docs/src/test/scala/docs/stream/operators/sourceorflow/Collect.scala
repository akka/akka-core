/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
object Collect {
  //#collect-elements
  trait Message
  final case class Ping(id: Int) extends Message
  final case class Pong(id: Int)
  //#collect-elements

  def collectExample(): Unit = {
    //#collect
    val flow: Flow[Message, Pong, NotUsed] =
      Flow[Message].collect {
        case Ping(id) if id != 0 => Pong(id)
      }
    //#collect
  }

  def collectType(): Unit = {
    //#collectType
    val flow: Flow[Message, Pong, NotUsed] =
      Flow[Message].collectType[Ping].filter(_.id != 0).map(p => Pong(p.id))
    //#collectType
  }
}
