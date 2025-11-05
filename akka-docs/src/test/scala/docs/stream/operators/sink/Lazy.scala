/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sink

import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

object Lazy {

  implicit val system: ActorSystem = ???

  def example(): Unit = {
    // #simple-example
    val matVal =
      Source
        .maybe[String]
        .map { element =>
          println(s"mapped $element")
          element
        }
        .toMat(Sink.lazySink { () =>
          println("Sink created")
          Sink.foreach(elem => println(s"foreach $elem"))
        })(Keep.left)
        .run()

    // some time passes
    // nothing has been printed
    matVal.success(Some("one"))
    // now prints:
    // mapped one
    // Sink created
    // foreach one

    // #simple-example
  }
}
