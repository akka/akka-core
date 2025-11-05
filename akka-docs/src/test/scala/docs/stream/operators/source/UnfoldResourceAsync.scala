/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.Future

object UnfoldResourceAsync {

  // imaginary async API we need to use
  // #unfoldResource-async-api
  trait Database {
    // blocking query
    def doQuery(): Future[QueryResult]
  }
  trait QueryResult {
    def hasMore(): Future[Boolean]
    def nextEntry(): Future[DatabaseEntry]
    def close(): Future[Unit]
  }
  trait DatabaseEntry
  // #unfoldResource-async-api

  def unfoldResourceExample(): Unit = {
    implicit val actorSystem = ActorSystem()
    implicit val ex = actorSystem.dispatcher
    // #unfoldResourceAsync
    // we don't actually have one, it was just made up for the sample
    val database: Database = ???

    val queryResultSource: Source[DatabaseEntry, NotUsed] =
      Source.unfoldResourceAsync[DatabaseEntry, QueryResult](
        // open
        () => database.doQuery(),
        // read
        query =>
          query.hasMore().flatMap {
            case false => Future.successful(None)
            case true  => query.nextEntry().map(dbEntry => Some(dbEntry))
          },
        // close
        query => query.close().map(_ => Done))

    // process each element
    queryResultSource.runForeach(println)
    // #unfoldResourceAsync
  }

}
