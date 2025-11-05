/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.event.Level

import akka.actor
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._

class LogMessagesSpec extends ScalaTestWithActorTestKit("""
    akka.loglevel = DEBUG # test verifies debug
    """) with AnyWordSpecLike with LogCapturing {

  implicit val classic: actor.ActorSystem = system.toClassic

  "The log messages behavior" should {

    "log messages and signals" in {
      val behavior: Behavior[String] = Behaviors.logMessages(Behaviors.ignore)

      val ref: ActorRef[String] = spawn(behavior)

      LoggingTestKit.debug(s"actor [${ref.path.toString}] received message: Hello").expect {
        ref ! "Hello"
      }

      LoggingTestKit.debug(s"actor [${ref.path}] received signal: PostStop").expect {
        testKit.stop(ref)
      }
    }

    "log messages with provided log level" in {
      val opts = LogOptions().withLevel(Level.INFO)
      val behavior: Behavior[String] = Behaviors.logMessages(opts, Behaviors.ignore)

      val ref: ActorRef[String] = spawn(behavior)

      LoggingTestKit.info(s"actor [${ref.path}] received message: Hello").expect {
        ref ! "Hello"
      }

      LoggingTestKit.info(s"actor [${ref.path}] received signal: PostStop").expect {
        testKit.stop(ref)
      }
    }

    "log messages with provided logger" in {
      val logger = system.log
      val opts = LogOptions().withLogger(logger)
      val behavior: Behavior[String] = Behaviors.logMessages(opts, Behaviors.ignore)

      val ref: ActorRef[String] = spawn(behavior)

      LoggingTestKit.debug(s"actor [${ref.path}] received message: Hello").expect {
        ref ! "Hello"
      }

      LoggingTestKit.debug(s"actor [${ref.path}] received signal: PostStop").expect {
        testKit.stop(ref)
      }
    }

    "not log messages when not enabled" in {
      val opts = LogOptions().withEnabled(false)
      val behavior: Behavior[String] = Behaviors.logMessages(opts, Behaviors.ignore)

      val ref: ActorRef[String] = spawn(behavior)

      LoggingTestKit.debug(s"actor [${ref.path}] received message: Hello").withOccurrences(0).expect {
        ref ! "Hello"
      }

      LoggingTestKit.debug(s"actor [${ref.path}] received signal: PostStop").withOccurrences(0).expect {
        testKit.stop(ref)
      }
    }

    "log messages with decorated MDC values" in {
      val opts = LogOptions().withLevel(Level.DEBUG)
      val mdc = Map("mdc" -> "true")
      val behavior = Behaviors.withMdc[String](mdc)(Behaviors.logMessages(opts, Behaviors.ignore))

      val ref = spawn(behavior)

      LoggingTestKit.debug(s"actor [${ref.path}] received message: Hello").withMdc(mdc).expect {
        ref ! "Hello"
      }

      LoggingTestKit.debug(s"actor [${ref.path}] received signal: PostStop").withMdc(mdc).expect {
        testKit.stop(ref)
      }
    }

    "log messages with different decorated MDC values in different actors" in {
      val opts = LogOptions().withLevel(Level.DEBUG)
      val mdc1 = Map("mdc" -> "true")
      val behavior1 = Behaviors.withMdc[String](mdc1)(Behaviors.logMessages(opts, Behaviors.ignore))
      val mdc2 = Map("mdc" -> "false")
      val behavior2 = Behaviors.withMdc[String](mdc2)(Behaviors.logMessages(opts, Behaviors.ignore))

      val ref2 = spawn(behavior2)

      LoggingTestKit.debug(s"actor [${ref2.path}] received message: Hello").withMdc(mdc2).expect {
        ref2 ! "Hello"
      }

      val ref1 = spawn(behavior1)

      LoggingTestKit.debug(s"actor [${ref1.path}] received message: Hello").withMdc(mdc1).expect {
        ref1 ! "Hello"
      }

      LoggingTestKit.debug(s"actor [${ref2.path}] received signal: PostStop").withMdc(mdc2).expect {
        testKit.stop(ref2)
      }

      LoggingTestKit.debug(s"actor [${ref1.path}] received signal: PostStop").withMdc(mdc1).expect {
        testKit.stop(ref1)
      }
    }

    "log messages of different type" in {
      val behavior: Behavior[String] = Behaviors.logMessages(Behaviors.ignore[String])

      val ref = spawn(behavior)

      LoggingTestKit.debug(s"actor [${ref.path}] received message: 13").expect {
        ref.unsafeUpcast[Any] ! 13
      }
    }

  }
}
