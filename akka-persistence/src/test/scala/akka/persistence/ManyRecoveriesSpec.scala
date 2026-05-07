/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.testkit._

object ManyRecoveriesSpec {

  def testProps(name: String, latch: Option[TestLatch]): Props =
    Props(new TestPersistentActor(name, latch))

  final case class Cmd(s: String)
  final case class Evt(s: String)

  class TestPersistentActor(name: String, latch: Option[TestLatch]) extends PersistentActor {

    override def persistenceId = name

    override def receiveRecover: Receive = {
      case Evt(_) =>
        latch.foreach(Await.ready(_, 10.seconds))
    }
    override def receiveCommand: Receive = {
      case Cmd(s) =>
        persist(Evt(s)) { _ =>
          sender() ! s"$persistenceId-$s-${lastSequenceNr}"
        }
      case "stop" =>
        context.stop(self)
    }
  }

}

class ManyRecoveriesSpec
    extends PersistenceSpec(ConfigFactory.parseString("""
    akka.actor.default-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 5
      }
    }
    akka.persistence.max-concurrent-recoveries = 3
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.actor.warn-about-java-serializer-usage = off
  """))
    with ImplicitSender {
  import ManyRecoveriesSpec._

  "Many persistent actors" must {
    "be able to recovery without overloading" in {
      (1 to 100).foreach { n =>
        system.actorOf(testProps(s"a$n", latch = None)) ! Cmd("A")
        expectMsg(s"a$n-A-1")
      }

      // this would starve (block) all threads without max-concurrent-recoveries
      val latch = TestLatch()
      (1 to 100).foreach { n =>
        system.actorOf(testProps(s"a$n", Some(latch))) ! Cmd("B")
      }
      // this should be able to progress even though above is blocking,
      // 2 remaining non-blocked threads
      (1 to 10).foreach { n =>
        system.actorOf(TestActors.echoActorProps) ! n
        expectMsg(n)
      }

      latch.countDown()
      receiveN(100).toSet should ===((1 to 100).map(n => s"a$n-B-2").toSet)
    }
  }

}
