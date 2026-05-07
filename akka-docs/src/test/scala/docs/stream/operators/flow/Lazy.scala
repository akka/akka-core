/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.flow

import java.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

object Lazy {

  implicit val system: ActorSystem = ???

  def example(): Unit = {
    // #simple-example
    val numbers = Source
      .unfold(0) { n =>
        val next = n + 1
        println(s"Source producing $next")
        Some((next, next))
      }
      .take(3)

    val flow = Flow.lazyFlow { () =>
      println("Creating the actual flow")
      Flow[Int].map { element =>
        println(s"Actual flow mapped $element")
        element
      }
    }

    numbers.via(flow).run()
    // prints:
    // Source producing 1
    // Creating the actual flow
    // Actual flow mapped 1
    // Source producing 2
    // Actual flow mapped 2
    // #simple-example
  }

  def statefulMap(): Unit = {
    // #mutable-example
    val mutableFold = Flow.lazyFlow { () =>
      val zero = new util.ArrayList[Int]()
      Flow[Int].fold(zero) { (list, element) =>
        list.add(element)
        list
      }
    }
    val stream =
      Source(1 to 3).via(mutableFold).to(Sink.foreach(println))

    stream.run()
    stream.run()
    stream.run()
    // prints:
    // [1, 2, 3]
    // [1, 2, 3]
    // [1, 2, 3]

    // #mutable-example
  }

}
