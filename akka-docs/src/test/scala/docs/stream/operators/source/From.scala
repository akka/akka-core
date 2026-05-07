/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import java.util.stream.IntStream

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

object From {

  implicit val system: ActorSystem = null

  def fromIteratorSample(): Unit = {
    //#from-iterator
    Source.fromIterator(() => (1 to 3).iterator).runForeach(println)
    // could print
    // 1
    // 2
    // 3
    //#from-iterator
  }

  def fromJavaStreamSample(): Unit = {
    //#from-javaStream
    Source.fromJavaStream(() => IntStream.rangeClosed(1, 3)).runForeach(println)
    // could print
    // 1
    // 2
    // 3
    //#from-javaStream
  }

}
