/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sink

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }

import scala.concurrent.{ ExecutionContextExecutor, Future }

object Fold {
  implicit val system: ActorSystem = ???
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  def foldExample: Future[Unit] = {
    //#fold
    val source = Source(1 to 100)
    val result: Future[Int] = source.runWith(Sink.fold(0)((acc, element) => acc + element))
    result.map(println)
    //5050
    //#fold
  }
}
