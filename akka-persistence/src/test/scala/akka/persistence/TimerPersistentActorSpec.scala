/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.concurrent.duration._
import scala.runtime.BoxedUnit

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.japi.Procedure
import akka.testkit.{ EventFilter, ImplicitSender }
import akka.testkit.TestEvent.Mute

object TimerPersistentActorSpec {

  def testProps(name: String): Props =
    Props(new TestPersistentActor(name))

  final case class Scheduled(msg: Any, replyTo: ActorRef)

  final case class AutoReceivedMessageWrapper(msg: AutoReceivedMessage)

  class TestPersistentActor(name: String) extends Timers with PersistentActor {

    override def persistenceId = name

    override def receiveRecover: Receive = {
      case _ =>
    }

    override def receiveCommand: Receive = {
      case Scheduled(msg, replyTo) =>
        replyTo ! msg
      case AutoReceivedMessageWrapper(_) =>
        timers.startSingleTimer("PoisonPill", PoisonPill, Duration.Zero)
      case msg =>
        timers.startSingleTimer("key", Scheduled(msg, sender()), Duration.Zero)
        persist(msg)(_ => ())
    }
  }

  // this should fail in constructor
  class WrongOrder extends PersistentActor with Timers {
    override def persistenceId = "notused"
    override def receiveRecover: Receive = {
      case _ =>
    }
    override def receiveCommand: Receive = {
      case _ => ()
    }
  }

  def testJavaProps(name: String): Props =
    Props(new JavaTestPersistentActor(name))

  class JavaTestPersistentActor(name: String) extends AbstractPersistentActorWithTimers {

    override def persistenceId: String = name

    override def createReceiveRecover(): AbstractActor.Receive =
      AbstractActor.emptyBehavior

    override def createReceive(): AbstractActor.Receive =
      new AbstractActor.Receive({
        case Scheduled(msg, replyTo) =>
          replyTo ! msg
          BoxedUnit.UNIT
        case msg =>
          timers.startSingleTimer("key", Scheduled(msg, sender()), Duration.Zero)
          persist(msg, new Procedure[Any] {
            override def apply(evt: Any): Unit = ()
          })
          BoxedUnit.UNIT
      })
  }

}

class TimerPersistentActorSpec
    extends PersistenceSpec(ConfigFactory.parseString("""
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.actor.warn-about-java-serializer-usage = off
  """))
    with ImplicitSender {
  import TimerPersistentActorSpec._

  system.eventStream.publish(Mute(EventFilter[ActorInitializationException]()))

  "PersistentActor with Timer" must {
    "not discard timer msg due to stashing" in {
      val pa = system.actorOf(testProps("p1"))
      pa ! "msg1"
      expectMsg("msg1")
    }

    "not discard timer msg due to stashing for AbstractPersistentActorWithTimers" in {
      val pa = system.actorOf(testJavaProps("p2"))
      pa ! "msg2"
      expectMsg("msg2")
    }

    "reject wrong order of traits, PersistentActor with Timer" in {
      if (TraitOrder.canBeChecked) {
        val pa = system.actorOf(Props[WrongOrder]())
        watch(pa)
        expectTerminated(pa)
      }
    }

    "handle AutoReceivedMessage's automatically" in {
      val pa = system.actorOf(testProps("p3"))
      watch(pa)
      pa ! AutoReceivedMessageWrapper(PoisonPill)
      expectTerminated(pa)
    }

  }

}
