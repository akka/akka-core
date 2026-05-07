/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.actor.Status.Failure
import akka.persistence.journal.SteppingInmemJournal
import akka.testkit.{ AkkaSpec, ImplicitSender, TestProbe }

object PersistentActorRecoveryTimeoutSpec {
  val journalId = "persistent-actor-recovery-timeout-spec"

  def config =
    SteppingInmemJournal
      .config(PersistentActorRecoveryTimeoutSpec.journalId)
      .withFallback(ConfigFactory.parseString("""
          |akka.persistence.journal.stepping-inmem.recovery-event-timeout=1s
        """.stripMargin))
      .withFallback(PersistenceSpec.config("stepping-inmem", "PersistentActorRecoveryTimeoutSpec"))

  class TestActor(probe: ActorRef) extends NamedPersistentActor("recovery-timeout-actor") {
    override def receiveRecover: Receive = Actor.emptyBehavior

    override def receiveCommand: Receive = {
      case x =>
        persist(x) { _ =>
          sender() ! x
        }
    }

    override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      probe ! Failure(cause)
    }
  }

  class TestReceiveTimeoutActor(receiveTimeout: FiniteDuration, probe: ActorRef)
      extends NamedPersistentActor("recovery-timeout-actor-2")
      with ActorLogging {

    override def preStart(): Unit = {
      context.setReceiveTimeout(receiveTimeout)
    }

    override def receiveRecover: Receive = {
      case RecoveryCompleted => probe ! context.receiveTimeout
      case _                 => // we don't care
    }

    override def receiveCommand: Receive = {
      case x =>
        persist(x) { _ =>
          sender() ! x
        }
    }

    override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      log.error(cause, "Recovery of TestReceiveTimeoutActor failed")
      probe ! Failure(cause)
    }
  }

}

class PersistentActorRecoveryTimeoutSpec
    extends AkkaSpec(PersistentActorRecoveryTimeoutSpec.config)
    with ImplicitSender {

  import PersistentActorRecoveryTimeoutSpec.journalId

  "The recovery timeout" should {

    "fail recovery if timeout is not met when recovering" in {
      val probe = TestProbe()
      val persisting = system.actorOf(Props(classOf[PersistentActorRecoveryTimeoutSpec.TestActor], probe.ref))

      awaitAssert(SteppingInmemJournal.getRef(journalId), 3.seconds)
      val journal = SteppingInmemJournal.getRef(journalId)

      // initial read highest
      SteppingInmemJournal.step(journal)

      persisting ! "A"
      SteppingInmemJournal.step(journal)
      expectMsg("A")

      watch(persisting)
      system.stop(persisting)
      expectTerminated(persisting)

      // now replay, but don't give the journal any tokens to replay events
      // so that we cause the timeout to trigger
      val replaying = system.actorOf(Props(classOf[PersistentActorRecoveryTimeoutSpec.TestActor], probe.ref))
      watch(replaying)

      // initial read highest
      SteppingInmemJournal.step(journal)

      probe.expectMsgType[Failure].cause shouldBe a[RecoveryTimedOut]
      expectTerminated(replaying)

      // avoid having it stuck in the next test from the
      // last read request above
      SteppingInmemJournal.step(journal)
    }

    "should not interfere with receive timeouts" in {
      val probe1 = TestProbe()
      val persisting =
        system.actorOf(Props(classOf[PersistentActorRecoveryTimeoutSpec.TestReceiveTimeoutActor], 42.days, probe1.ref))

      awaitAssert(SteppingInmemJournal.getRef(journalId), 3.seconds)
      val journal = SteppingInmemJournal.getRef(journalId)

      // initial read highest
      SteppingInmemJournal.step(journal)

      persisting ! "A"
      SteppingInmemJournal.step(journal)
      expectMsg("A")

      watch(persisting)
      system.stop(persisting)
      expectTerminated(persisting)

      // now replay
      val probe2 = TestProbe()
      val timeout = 20.millis
      val replaying =
        system.actorOf(Props(classOf[PersistentActorRecoveryTimeoutSpec.TestReceiveTimeoutActor], timeout, probe2.ref))

      probe2.expectNoMessage(50.millis) // longer than the receive timeout
      // initial read highest
      SteppingInmemJournal.step(journal)
      probe2.expectNoMessage(100.millis) // receive timeout should not trigger recovery timeout

      // but waiting longer without SteppingInmemJournal.step will be recovery timeout
      probe2.expectMsgType[Failure].cause shouldBe a[RecoveryTimedOut]
      watch(replaying)
      expectTerminated(replaying)

      // avoid having it stuck in the next test from the
      // last read request above
      SteppingInmemJournal.step(journal)

    }

  }

}
