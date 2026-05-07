/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=never used") // sample snippets
object LimitWeighted {

  implicit val system: ActorSystem[_] = ???

  def simple(): Unit = {
    // #simple
    val untrustedSource: Source[ByteString, NotUsed] = Source.repeat(ByteString("element"))

    val allBytes: Future[ByteString] =
      untrustedSource.limitWeighted(max = 10000)(_.length).runReduce(_ ++ _)
    // #simple
  }

}
