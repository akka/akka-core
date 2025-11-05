/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

import scala.concurrent.Future

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
