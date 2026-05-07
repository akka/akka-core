/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sink

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }

import scala.concurrent.ExecutionContextExecutor

object Cancelled {
  implicit val system: ActorSystem = ???
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  def cancelledExample(): NotUsed = {
    //#cancelled
    val source = Source(1 to 5)
    source.runWith(Sink.cancelled)
    //#cancelled
  }
}
