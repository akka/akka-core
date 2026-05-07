/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContextExecutor, Future }

object CompletionTimeout {
  implicit val system: ActorSystem = ???
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  def completionTimeoutExample: Future[Done] = {
    //#completionTimeout
    val source = Source(1 to 10000).map(number => number * number)
    source.completionTimeout(10.milliseconds).run()
    //#completionTimeout
  }
}
