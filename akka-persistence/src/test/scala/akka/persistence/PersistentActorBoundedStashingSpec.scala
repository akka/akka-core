/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.concurrent.duration._
import com.typesafe.config.Config
import org.scalatest.BeforeAndAfterEach
import akka.actor.DeadLetter
import akka.persistence.PersistentActorBoundedStashingSpec._
import akka.persistence.journal.SteppingInmemJournal
import akka.testkit.EventFilter
import akka.testkit.ImplicitSender
import akka.testkit.TestEvent.Mute

import scala.annotation.nowarn

object PersistentActorBoundedStashingSpec {
  final case class Cmd(data: Any)
  final case class Evt(data: Any)

  class ReplyToWithRejectConfigurator extends StashOverflowStrategyConfigurator {
    override def create(config: Config): StashOverflowStrategy = ReplyToStrategy("RejectToStash")
  }

  class StashOverflowStrategyFromConfigPersistentActor(name: String) extends NamedPersistentActor(name) {
    var events: List[Any] = Nil

    val updateState: Receive = {
      case Evt(data) => events = data :: events
    }

    val commonBehavior: Receive = {
      case GetState => sender() ! events.reverse
    }

    def receiveRecover = updateState

    override def receiveCommand: Receive = commonBehavior.orElse {
      case Cmd(x: Any) => persist(Evt(x))(updateState)
    }
  }

  val capacity = 10

  val templateConfig =
    s"""
       |akka.actor.default-mailbox.stash-capacity=$capacity
       |akka.actor.guardian-supervisor-strategy="akka.actor.StoppingSupervisorStrategy"
       |akka.persistence.internal-stash-overflow-strategy = "%s"
       |""".stripMargin

  val throwConfig = String.format(templateConfig, "akka.persistence.ThrowExceptionConfigurator")
  val discardConfig = String.format(templateConfig, "akka.persistence.DiscardConfigurator")
  val replyToConfig =
    String.format(templateConfig, "akka.persistence.PersistentActorBoundedStashingSpec$ReplyToWithRejectConfigurator")

}

class SteppingInMemPersistentActorBoundedStashingSpec(strategyConfig: String)
    extends PersistenceSpec(
      SteppingInmemJournal
        .config("persistence-bounded-stash")
        .withFallback(
          PersistenceSpec.config(
            "stepping-inmem",
            "SteppingInMemPersistentActorBoundedStashingSpec",
            extraConfig = Some(strategyConfig))))
    with BeforeAndAfterEach
    with ImplicitSender {

  override def atStartup(): Unit = {
    system.eventStream.publish(Mute(EventFilter.warning(pattern = ".*received dead letter from.*Cmd.*")))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    system.eventStream.subscribe(testActor, classOf[DeadLetter])
  }

  override def afterEach(): Unit =
    system.eventStream.unsubscribe(testActor, classOf[DeadLetter])

}

class ThrowExceptionStrategyPersistentActorBoundedStashingSpec
    extends SteppingInMemPersistentActorBoundedStashingSpec(PersistentActorBoundedStashingSpec.throwConfig) {
  "Stashing with ThrowOverflowExceptionStrategy in a persistence actor " should {
    "throws stash overflow exception" in {
      val persistentActor = namedPersistentActor[StashOverflowStrategyFromConfigPersistentActor]
      awaitAssert(SteppingInmemJournal.getRef("persistence-bounded-stash"), 3.seconds)
      val journal = SteppingInmemJournal.getRef("persistence-bounded-stash")

      // initial read highest
      SteppingInmemJournal.step(journal)

      // make sure it's fully started first
      persistentActor ! GetState
      expectMsg(Nil)

      //barrier for stash
      persistentActor ! Cmd("a")

      //internal stash overflow
      (1 to (capacity + 1)).foreach(persistentActor ! Cmd(_))

      //after PA stopped, all stashed messages forward to deadletters
      //the message triggering the overflow is lost, so we get one less message than we sent
      (1 to capacity).foreach(i => expectMsg(DeadLetter(Cmd(i), testActor, persistentActor)))

      // send another message to the now dead actor and make sure that it goes to dead letters
      persistentActor ! Cmd(capacity + 2)
      expectMsg(DeadLetter(Cmd(capacity + 2), testActor, persistentActor))
    }
  }
}

@nowarn("cat=lint-infer-any")
class DiscardStrategyPersistentActorBoundedStashingSpec
    extends SteppingInMemPersistentActorBoundedStashingSpec(PersistentActorBoundedStashingSpec.discardConfig) {
  "Stashing with DiscardToDeadLetterStrategy in a persistence actor " should {
    "discard to deadletter" in {
      val persistentActor = namedPersistentActor[StashOverflowStrategyFromConfigPersistentActor]
      awaitAssert(SteppingInmemJournal.getRef("persistence-bounded-stash"), 3.seconds)
      val journal = SteppingInmemJournal.getRef("persistence-bounded-stash")

      //initial read highest
      SteppingInmemJournal.step(journal)

      // make sure it's fully started first
      persistentActor ! GetState
      expectMsg(Nil)

      //barrier for stash
      persistentActor ! Cmd("a")

      //internal stash overflow after 10
      (1 to (2 * capacity)).foreach(persistentActor ! Cmd(_))
      //so, 11 to 20 discard to deadletter
      ((1 + capacity) to (2 * capacity)).foreach(i => expectMsg(DeadLetter(Cmd(i), testActor, persistentActor)))
      //allow "a" and 1 to 10 write complete
      (1 to (1 + capacity)).foreach(_ => SteppingInmemJournal.step(journal))

      persistentActor ! GetState

      expectMsg("a" :: (1 to capacity).toList ::: Nil)
    }
  }
}

@nowarn("cat=lint-infer-any")
class ReplyToStrategyPersistentActorBoundedStashingSpec
    extends SteppingInMemPersistentActorBoundedStashingSpec(PersistentActorBoundedStashingSpec.replyToConfig) {
  "Stashing with DiscardToDeadLetterStrategy in a persistence actor" should {
    "reply to request with custom message" in {
      val persistentActor = namedPersistentActor[StashOverflowStrategyFromConfigPersistentActor]
      awaitAssert(SteppingInmemJournal.getRef("persistence-bounded-stash"), 3.seconds)
      val journal = SteppingInmemJournal.getRef("persistence-bounded-stash")

      //initial read highest
      SteppingInmemJournal.step(journal)

      // make sure it's fully started first
      persistentActor ! GetState
      expectMsg(Nil)

      //barrier for stash
      persistentActor ! Cmd("a")

      //internal stash overflow after 10
      (1 to (2 * capacity)).foreach(persistentActor ! Cmd(_))
      //so, 11 to 20 reply to with "Reject" String
      ((1 + capacity) to (2 * capacity)).foreach(_ => expectMsg("RejectToStash"))
      //allow "a" and 1 to 10 write complete
      (1 to (1 + capacity)).foreach(_ => SteppingInmemJournal.step(journal))

      persistentActor ! GetState

      expectMsg("a" :: (1 to capacity).toList ::: Nil)
    }
  }
}
