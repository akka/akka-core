/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration._

@nowarn("msg=never used") // sample snippets
object UnfoldAsync {

  // #unfoldAsync-actor-protocol
  object DataActor {
    sealed trait Command
    case class FetchChunk(offset: Long, replyTo: ActorRef[Chunk]) extends Command
    case class Chunk(bytes: ByteString)
    // #unfoldAsync-actor-protocol

  }
  implicit val system: ActorSystem[Nothing] = ???

  def unfoldAsyncExample(): Unit = {
    // #unfoldAsync
    // actor we can query for data with an offset
    val dataActor: ActorRef[DataActor.Command] = ???
    import system.executionContext

    implicit val askTimeout: Timeout = 3.seconds
    val startOffset = 0L
    val byteSource: Source[ByteString, NotUsed] =
      Source.unfoldAsync(startOffset) { currentOffset =>
        // ask for next chunk
        val nextChunkFuture: Future[DataActor.Chunk] =
          dataActor.ask(DataActor.FetchChunk(currentOffset, _))

        nextChunkFuture.map { chunk =>
          val bytes = chunk.bytes
          if (bytes.isEmpty) None // end of data
          else Some((currentOffset + bytes.length, bytes))
        }
      }
    // #unfoldAsync
  }

}
