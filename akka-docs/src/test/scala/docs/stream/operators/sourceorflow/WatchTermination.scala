/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

object WatchTermination {

  def watchTerminationExample(): Unit = {
    implicit val system: ActorSystem = ???
    implicit val ec: ExecutionContext = ???

    //#watchTermination
    Source(1 to 5)
      .watchTermination()(
        (prevMatValue, future) =>
          // this function will be run when the stream terminates
          // the Future provided as a second parameter indicates whether the stream completed successfully or failed
          future.onComplete {
            case Failure(exception) => println(exception.getMessage)
            case Success(_)         => println(s"The stream materialized $prevMatValue")
          })
      .runForeach(println)
    /*
    Prints:
    1
    2
    3
    4
    5
    The stream materialized NotUsed
     */

    Source(1 to 5)
      .watchTermination()((prevMatValue, future) =>
        future.onComplete {
          case Failure(exception) => println(exception.getMessage)
          case Success(_)         => println(s"The stream materialized $prevMatValue")
        })
      .runForeach(e => if (e == 3) throw new Exception("Boom") else println(e))
    /*
    Prints:
    1
    2
    Boom
     */
    //#watchTermination
  }
}
