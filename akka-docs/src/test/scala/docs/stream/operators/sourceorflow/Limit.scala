/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=never used") // sample snippets
object Limit {

  implicit val system: ActorSystem[_] = ???

  def simple(): Unit = {
    // #simple
    val untrustedSource: Source[String, NotUsed] = Source.repeat("element")

    val elements: Future[Seq[String]] =
      untrustedSource.limit(10000).runWith(Sink.seq)
    // #simple
  }

}
