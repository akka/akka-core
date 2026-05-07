/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import java.net.URL

object MapWithResource {
  implicit val actorSystem: ActorSystem = ???

  //#mapWithResource-blocking-api
  trait DBDriver {
    def create(url: URL, userName: String, password: String): Connection
  }
  trait Connection {
    def close(): Unit
  }
  trait Database {
    //blocking query
    def doQuery(connection: Connection, query: String): QueryResult = ???
  }
  trait QueryResult {
    def hasMore: Boolean
    // potentially blocking retrieval of each element
    def next(): DataBaseRecord
    // potentially blocking retrieval all element
    def toList(): List[DataBaseRecord]
  }
  trait DataBaseRecord
  //#mapWithResource-blocking-api
  val url: URL = ???
  val userName = "Akka"
  val password = "Hakking"
  val dbDriver: DBDriver = ???
  def mapWithResourceExample(): Unit = {
    //#mapWithResource
    //some database for JVM
    val db: Database = ???
    Source(
      List(
        "SELECT * FROM shop ORDER BY article-0000 order by gmtModified desc limit 100;",
        "SELECT * FROM shop ORDER BY article-0001 order by gmtModified desc limit 100;"))
      .mapWithResource(() => dbDriver.create(url, userName, password))(
        (connection, query) => db.doQuery(connection, query).toList(),
        conn => {
          conn.close()
          None
        })
      .mapConcat(identity)
      .runForeach(println)
    //#mapWithResource
  }
}
