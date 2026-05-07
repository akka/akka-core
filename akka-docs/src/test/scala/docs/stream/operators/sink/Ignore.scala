/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sink

import java.util.UUID
import scala.concurrent.Future
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

import scala.annotation.nowarn

object Ignore {
  implicit val system: ActorSystem = ???

  def ignoreExample(): Unit = {
    //#ignore
    val lines: Source[String, NotUsed] = readLinesFromFile()
    val databaseIds: Source[UUID, NotUsed] =
      lines.mapAsync(1)(line => saveLineToDatabase(line))
    databaseIds.mapAsync(1)(uuid => writeIdToFile(uuid)).runWith(Sink.ignore)
    //#ignore
  }

  private def readLinesFromFile(): Source[String, NotUsed] =
    Source.empty

  @nowarn("msg=never used") // sample snippets
  private def saveLineToDatabase(line: String): Future[UUID] =
    Future.successful(UUID.randomUUID())

  private def writeIdToFile(uuid: UUID): Future[UUID] =
    Future.successful(uuid)

}
