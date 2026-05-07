/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler
import akka.testkit.TestLatch

object ManyRecoveriesSpec {

  sealed case class Cmd(s: String)

  final case class Evt(s: String)

  def persistentBehavior(
      name: String,
      probe: TestProbe[String],
      latch: Option[TestLatch]): EventSourcedBehavior[Cmd, Evt, String] =
    EventSourcedBehavior[Cmd, Evt, String](
      persistenceId = PersistenceId.ofUniqueId(name),
      emptyState = "",
      commandHandler = CommandHandler.command {
        case Cmd(s) => Effect.persist(Evt(s)).thenRun(_ => probe.ref ! s"$name-$s")
      },
      eventHandler = {
        case (state, _) => latch.foreach(Await.ready(_, 10.seconds)); state
      })

  def forwardBehavior(sender: TestProbe[String]): Behaviors.Receive[Int] =
    Behaviors.receiveMessagePartial[Int] {
      case value =>
        sender.ref ! value.toString
        Behaviors.same
    }

  def forN(n: Int)(mapper: Int => String): Set[String] =
    (1 to n).map(mapper).toSet
}

class ManyRecoveriesSpec
    extends ScalaTestWithActorTestKit("""
    akka.actor.default-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 5
      }
    }
    akka.persistence.max-concurrent-recoveries = 3
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    """)
    with AnyWordSpecLike
    with LogCapturing {

  import ManyRecoveriesSpec._

  "Many persistent actors" must {
    "be able to recover without overloading" in {
      val probe = TestProbe[String]()
      (1 to 100).foreach { n =>
        val name = s"a$n"
        spawn(persistentBehavior(s"a$n", probe, latch = None), name) ! Cmd("A")
        probe.expectMessage(s"a$n-A")
      }

      // this would starve (block) all threads without max-concurrent-recoveries
      val latch = TestLatch()(system.toClassic)
      (1 to 100).foreach { n =>
        spawn(persistentBehavior(s"a$n", probe, Some(latch))) ! Cmd("B")
      }
      // this should be able to progress even though above is blocking,
      // 2 remaining non-blocked threads
      (1 to 10).foreach { n =>
        spawn(forwardBehavior(probe)) ! n
        probe.expectMessage(n.toString)
      }

      latch.countDown()

      forN(100)(_ => probe.receiveMessage()) should
      be(forN(100)(i => s"a$i-B"))
    }
  }
}
