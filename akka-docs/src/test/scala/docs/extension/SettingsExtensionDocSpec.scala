/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.extension

//#imports
import akka.actor.ActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.ExtendedActorSystem

import scala.concurrent.duration.Duration
import com.typesafe.config.Config

import java.util.concurrent.TimeUnit
import akka.actor.ClassicActorSystemProvider

import scala.annotation.nowarn

//#imports

import akka.actor.Actor
import akka.testkit.AkkaSpec

//#extension
class SettingsImpl(config: Config) extends Extension {
  val DbUri: String = config.getString("myapp.db.uri")
  val CircuitBreakerTimeout: Duration =
    Duration(config.getDuration("myapp.circuit-breaker.timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
}
//#extension

//#extensionid
object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {

  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) =
    new SettingsImpl(system.settings.config)

  /**
   * Java API: retrieve the Settings extension for the given system.
   */
  override def get(system: ActorSystem): SettingsImpl = super.get(system)
  override def get(system: ClassicActorSystemProvider): SettingsImpl = super.get(system)
}
//#extensionid

@nowarn("msg=never used") // sample snippets
object SettingsExtensionDocSpec {

  val config = """
    //#config
    myapp {
      db {
        uri = "mongodb://example1.com:27017,example2.com:27017"
      }
      circuit-breaker {
        timeout = 30 seconds
      }
    }
    //#config
    """

  //#extension-usage-actor

  class MyActor extends Actor {
    val settings = Settings(context.system)
    val connection = connect(settings.DbUri, settings.CircuitBreakerTimeout)

    //#extension-usage-actor
    def receive = {
      case someMessage =>
    }

    def connect(dbUri: String, circuitBreakerTimeout: Duration) = {
      "dummy"
    }
  }

}

@nowarn("msg=never used") // sample snippets
class SettingsExtensionDocSpec extends AkkaSpec(SettingsExtensionDocSpec.config) {

  "demonstrate how to create application specific settings extension in Scala" in {
    //#extension-usage
    val dbUri = Settings(system).DbUri
    val circuitBreakerTimeout = Settings(system).CircuitBreakerTimeout
    //#extension-usage
  }

}
