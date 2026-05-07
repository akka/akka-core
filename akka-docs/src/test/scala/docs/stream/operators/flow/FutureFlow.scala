/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.flow

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=never used") // sample snippets
class FutureFlow {

  implicit val system: ActorSystem = ???
  import system.dispatcher

  def compileOnlyBaseOnFirst(): Unit = {
    // #base-on-first-element
    def processingFlow(id: Int): Future[Flow[Int, String, NotUsed]] =
      Future {
        Flow[Int].map(n => s"id: $id, value: $n")
      }

    val source: Source[String, NotUsed] =
      Source(1 to 10).prefixAndTail(1).flatMapConcat {
        case (List(id: Int), tail) =>
          // base the Future flow creation on the first element
          tail.via(Flow.futureFlow(processingFlow(id)))
      }
    // #base-on-first-element
  }

}
