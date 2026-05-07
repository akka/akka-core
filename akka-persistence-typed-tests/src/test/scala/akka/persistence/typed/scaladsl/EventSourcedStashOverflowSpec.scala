/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.journal.SteppingInmemJournal
import akka.persistence.typed.PersistenceId

// Reproducer for #29401
object EventSourcedStashOverflowSpec {

  object EventSourcedStringList {
    sealed trait Command
    case class DoNothing(replyTo: ActorRef[Done]) extends Command

    def apply(persistenceId: PersistenceId): Behavior[Command] =
      EventSourcedBehavior[Command, String, List[String]](
        persistenceId,
        Nil, { (_, command) =>
          command match {
            case DoNothing(replyTo) =>
              Effect.persist(List.empty[String]).thenRun(_ => replyTo ! Done)
          }
        }, { (state, event) =>
          // original reproducer slept 2 seconds here but a pure application of an event seems unlikely to take that long
          // so instead we delay recovery using a special journal
          event :: state
        })
  }

  def conf =
    SteppingInmemJournal
      .config("EventSourcedStashOverflow")
      .withFallback(ConfigFactory.parseString("""
       akka.persistence {
         typed {
           stash-capacity = 5000 # enough to fail on stack size
           stash-overflow-strategy = "drop"
         }
       }
   """))
}

class EventSourcedStashOverflowSpec
    extends ScalaTestWithActorTestKit(EventSourcedStashOverflowSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedStashOverflowSpec.EventSourcedStringList

  "Stashing in a busy event sourced behavior" must {

    "not cause stack overflow" in {
      val es = spawn(EventSourcedStringList(PersistenceId.ofUniqueId("id-1")))

      // wait for journal to start
      val probe = testKit.createTestProbe[Done]()
      probe.awaitAssert(SteppingInmemJournal.getRef("EventSourcedStashOverflow"), 3.seconds)
      val journal = SteppingInmemJournal.getRef("EventSourcedStashOverflow")

      val droppedMessageProbe = testKit.createDroppedMessageProbe()
      val stashCapacity = testKit.config.getInt("akka.persistence.typed.stash-capacity")

      for (_ <- 0 to (stashCapacity * 2)) {
        es.tell(EventSourcedStringList.DoNothing(probe.ref))
      }
      // capacity + 1 should mean that we get a dropped last message when all stash is filled
      // while the actor is stuck in replay because journal isn't responding
      droppedMessageProbe.receiveMessage()
      implicit val classicSystem: akka.actor.ActorSystem =
        testKit.system.toClassic
      // we only need to do this one step and recovery completes
      SteppingInmemJournal.step(journal)

      // exactly how many is racy but at least the first stash buffer full should complete
      probe.receiveMessages(stashCapacity)
    }

  }

}
