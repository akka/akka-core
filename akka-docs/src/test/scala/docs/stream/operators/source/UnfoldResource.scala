/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

object UnfoldResource {

  // imaginary blocking API we need to use
  // #unfoldResource-blocking-api
  trait Database {
    // blocking query
    def doQuery(): QueryResult
  }
  trait QueryResult {
    def hasMore: Boolean
    // potentially blocking retrieval of each element
    def nextEntry(): DatabaseEntry
    def close(): Unit
  }
  trait DatabaseEntry
  // #unfoldResource-blocking-api

  def unfoldResourceExample(): Unit = {
    implicit val actorSystem = ActorSystem()
    // #unfoldResource
    // we don't actually have one, it was just made up for the sample
    val database: Database = ???

    val queryResultSource: Source[DatabaseEntry, NotUsed] =
      Source.unfoldResource[DatabaseEntry, QueryResult](
        // open
        { () =>
          database.doQuery()
        },
        // read
        { query =>
          if (query.hasMore)
            Some(query.nextEntry())
          else
            // signals end of resource
            None
        },
        // close
        query => query.close())

    // process each element
    queryResultSource.runForeach(println)
    // #unfoldResource
  }

}
