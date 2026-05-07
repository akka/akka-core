/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.BehaviorInterceptor
import akka.actor.typed.TypedActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId

object DurableStateBehaviorInterceptorSpec {

  def conf: Config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = INFO
    """))

  def testBehavior(persistenceId: PersistenceId, probe: ActorRef[String]): Behavior[String] =
    Behaviors.setup { _ =>
      DurableStateBehavior[String, String](
        persistenceId,
        emptyState = "",
        commandHandler = (_, command) =>
          command match {
            case _ =>
              Effect.persist(command).thenRun(newState => probe ! newState)
          })
    }

}

class DurableStateBehaviorInterceptorSpec
    extends ScalaTestWithActorTestKit(DurableStateBehaviorInterceptorSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  import DurableStateBehaviorInterceptorSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()})")

  "DurableStateBehavior interceptor" must {

    "be possible to combine with another interceptor" in {
      val probe = createTestProbe[String]()
      val pid = nextPid()

      val toUpper = new BehaviorInterceptor[String, String] {
        override def aroundReceive(
            ctx: TypedActorContext[String],
            msg: String,
            target: BehaviorInterceptor.ReceiveTarget[String]): Behavior[String] = {
          target(ctx, msg.toUpperCase())
        }
      }

      val ref = spawn(Behaviors.intercept(() => toUpper)(testBehavior(pid, probe.ref)))

      ref ! "a"
      ref ! "bc"
      probe.expectMessage("A")
      probe.expectMessage("BC")
    }

    "be possible to combine with transformMessages" in {
      val probe = createTestProbe[String]()
      val pid = nextPid()
      val ref = spawn(testBehavior(pid, probe.ref).transformMessages[String] {
        case s => s.toUpperCase()
      })

      ref ! "a"
      ref ! "bc"
      probe.expectMessage("A")
      probe.expectMessage("BC")
    }

    "be possible to combine with MDC" in {
      val probe = createTestProbe[String]()
      val pid = nextPid()
      val ref = spawn(Behaviors.setup[String] { _ =>
        Behaviors.withMdc(
          staticMdc = Map("pid" -> pid.toString),
          mdcForMessage = (msg: String) => Map("msg" -> msg.toUpperCase())) {
          testBehavior(pid, probe.ref)
        }
      })

      ref ! "a"
      ref ! "bc"
      probe.expectMessage("a")
      probe.expectMessage("bc")

    }
  }
}
