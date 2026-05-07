/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed.extensions

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.scaladsl.Behaviors
import scala.annotation.nowarn
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future

//#shared-resource
class ExpensiveDatabaseConnection {
  def executeQuery(query: String): Future[Any] = ???
}
//#shared-resource

//#extension-id
object DatabasePool extends ExtensionId[DatabasePool] {
  // will only be called once
  def createExtension(system: ActorSystem[_]): DatabasePool = new DatabasePool(system)

  // Java API
  def get(system: ActorSystem[_]): DatabasePool = apply(system)
}
//#extension-id

@nowarn
//#extension
class DatabasePool(system: ActorSystem[_]) extends Extension {
  // database configuration can be loaded from config
  // from the actor system
  private val _connection = new ExpensiveDatabaseConnection()

  def connection(): ExpensiveDatabaseConnection = _connection
}
//#extension

object ExtensionDocSpec {
  val config = ConfigFactory.parseString("""
      #config      
      akka.actor.typed.extensions = ["docs.akka.extensions.DatabasePool"]
      #config
                                         """)

  val initialBehavior: Behavior[Any] = Behaviors.empty[Any]

  //#usage
  Behaviors.setup[Any] { ctx =>
    DatabasePool(ctx.system).connection().executeQuery("insert into...")
    initialBehavior
  }
  //#usage
}
