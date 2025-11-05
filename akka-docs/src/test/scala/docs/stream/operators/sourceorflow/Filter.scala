/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

object Filter {

  implicit val system: ActorSystem = ActorSystem()

  def filterExample(): Unit = {
    // #filter
    val words: Source[String, NotUsed] =
      Source(
        ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt " +
        "ut labore et dolore magna aliqua").split(" ").toList)

    val longWords: Source[String, NotUsed] = words.filter(_.length > 6)

    longWords.runForeach(println)
    // consectetur
    // adipiscing
    // eiusmod
    // incididunt
    // #filter
  }

  def filterNotExample(): Unit = {
    // #filterNot
    val words: Source[String, NotUsed] =
      Source(
        ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt " +
        "ut labore et dolore magna aliqua").split(" ").toList)

    val longWords: Source[String, NotUsed] = words.filterNot(_.length <= 6)

    longWords.runForeach(println)
    // consectetur
    // adipiscing
    // eiusmod
    // incididunt
    // #filterNot
  }
}
