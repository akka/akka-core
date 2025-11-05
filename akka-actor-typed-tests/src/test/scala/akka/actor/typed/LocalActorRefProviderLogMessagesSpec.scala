/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, LogCapturing, LoggingTestKit, ScalaTestWithActorTestKit }
import akka.actor.typed.internal.adapter.ActorSystemAdapter

object LocalActorRefProviderLogMessagesSpec {
  val config = """
    akka {
      loglevel = DEBUG # test verifies debug
      log-dead-letters = on
      actor {
        debug.unhandled = on
      }
    }
  """
}

class LocalActorRefProviderLogMessagesSpec
    extends ScalaTestWithActorTestKit(LocalActorRefProviderLogMessagesSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  "An LocalActorRefProvider" must {

    "logs on dedicated 'serialization' logger of unknown path" in {
      val provider = system.asInstanceOf[ActorSystemAdapter[_]].provider

      LoggingTestKit
        .debug("of unknown (invalid) path [dummy/path]")
        .withLoggerName("akka.actor.LocalActorRefProvider.Deserialization")
        .expect {
          provider.resolveActorRef("dummy/path")
        }
    }

    "logs on dedicated 'serialization' logger when path doesn't match existing actor" in {
      val provider = system.asInstanceOf[ActorSystemAdapter[_]].provider
      val invalidPath = provider.rootPath / "user" / "invalid"

      LoggingTestKit
        .debug("Resolve (deserialization) of path [user/invalid] doesn't match an active actor.")
        .withLoggerName("akka.actor.LocalActorRefProvider.Deserialization")
        .expect {
          provider.resolveActorRef(invalidPath)
        }
    }

    "logs on dedicated 'serialization' logger when of foreign path" in {

      val otherSystem = ActorTestKit("otherSystem").system.asInstanceOf[ActorSystemAdapter[_]]
      val invalidPath = otherSystem.provider.rootPath / "user" / "foo"

      val provider = system.asInstanceOf[ActorSystemAdapter[_]].provider
      try {
        LoggingTestKit
          .debug("Resolve (deserialization) of foreign path [akka://otherSystem/user/foo]")
          .withLoggerName("akka.actor.LocalActorRefProvider.Deserialization")
          .expect {
            provider.resolveActorRef(invalidPath)
          }
      } finally {
        ActorTestKit.shutdown(otherSystem)
      }
    }
  }
}
