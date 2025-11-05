/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sink

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }

import scala.concurrent.{ ExecutionContextExecutor, Future }

object Reduce {
  implicit val system: ActorSystem = ???
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  def reduceExample: Future[Unit] = {
    //#reduceExample
    val source = Source(1 to 100).reduce((acc, element) => acc + element)
    val result: Future[Int] = source.runWith(Sink.head)
    result.map(println)
    //5050
    //#reduceExample
  }
}
