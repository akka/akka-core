/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sink

import scala.concurrent.{ ExecutionContextExecutor }
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
object AsPublisher {
  implicit val system: ActorSystem = ???
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  def asPublisherExample() = {
    def asPublisherExample() = {
      //#asPublisher
      val source = Source(1 to 5)

      val publisher = source.runWith(Sink.asPublisher(false))
      Source.fromPublisher(publisher).runWith(Sink.foreach(println)) // 1 2 3 4 5
      Source
        .fromPublisher(publisher)
        .runWith(Sink.foreach(println)) //No output, because the source was not able to subscribe to the publisher.
      //#asPublisher
    }
  }
}
