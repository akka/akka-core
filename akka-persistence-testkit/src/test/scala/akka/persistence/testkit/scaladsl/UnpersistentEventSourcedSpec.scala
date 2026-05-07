/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.Done
import akka.actor.typed.Behavior
import akka.actor.typed.RecipientRef
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.RetentionCriteria

object UnpersistentEventSourcedSpec {
  object BehaviorUnderTest {
    sealed trait Command

    case class PersistDomainEvent(replyTo: RecipientRef[Done]) extends Command
    case class PersistDomainEventIfBefore(count: Int, replyTo: RecipientRef[Boolean]) extends Command
    case class PersistDomainEventAfter(count: Int, replyTo: RecipientRef[Done]) extends Command
    case class NotifyAfter(count: Int, notifyTo: RecipientRef[Done], replyTo: RecipientRef[Boolean]) extends Command
    case class GetSequenceNumber(replyTo: RecipientRef[Long]) extends Command
    case object SnapshotNow extends Command

    sealed trait Event
    case object DomainEvent extends Event
    case object SnapshotMade extends Event
    case class ObserverAdded(count: Int, notifyTo: RecipientRef[Done]) extends Event

    case class State(domainEvtCount: Int, notifyAfter: Map[Int, RecipientRef[Done]], nextNotifyAt: Int)

    val initialState = State(0, Map.empty, Int.MaxValue)

    def apply(id: String, recoveryDone: RecipientRef[Done]): Behavior[Command] =
      Behaviors.setup { context =>
        context.setLoggerName(s"entity-$id")

        EventSourcedBehavior[Command, Event, State](
          persistenceId = PersistenceId.ofUniqueId(id),
          emptyState = initialState,
          commandHandler = applyCommand(_, _, context),
          eventHandler = applyEvent(_, _))
          .receiveSignal {
            case (state, RecoveryCompleted) =>
              context.log.debug("Recovered state for id [{}] is [{}]", id, state)
              recoveryDone ! Done
          }
          .snapshotWhen {
            case (_, SnapshotMade, _) => true
            case _                    => false
          }
          .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 3, keepNSnapshots = 2))
          .withTagger {
            case DomainEvent => Set("domain")
            case _           => Set.empty
          }
      }

    private def applyCommand(state: State, cmd: Command, context: ActorContext[Command]): Effect[Event, State] = {
      def persistDomainEvent[Reply](replyTo: RecipientRef[Reply], reply: Reply): Effect[Event, State] =
        Effect
          .persist[Event, State](DomainEvent)
          .thenRun { newState =>
            state.notifyAfter.keysIterator
              .filter { at =>
                (at <= newState.nextNotifyAt) && !(newState.notifyAfter.isDefinedAt(at))
              }
              .foreach { at =>
                state.notifyAfter(at) ! Done
              }

            replyTo ! reply
          }
          .thenUnstashAll()

      cmd match {
        case PersistDomainEvent(replyTo) => persistDomainEvent(replyTo, Done)

        case PersistDomainEventIfBefore(count, replyTo) =>
          if (state.domainEvtCount >= count) {
            context.log.info("Rejecting PersistDomainEventIfBefore as domainEvtCount = {}", state.domainEvtCount)
            Effect.none.thenRun(_ => replyTo ! false)
          } else persistDomainEvent(replyTo, true)

        case PersistDomainEventAfter(count, replyTo) =>
          if (state.domainEvtCount < count) Effect.stash()
          else persistDomainEvent(replyTo, Done)

        case NotifyAfter(count, notifyTo, replyTo) =>
          if (state.domainEvtCount >= count) {
            Effect.none.thenRun { _ =>
              notifyTo ! Done
              replyTo ! true
            }
          } else if (state.notifyAfter.isDefinedAt(count)) {
            Effect.none.thenRun(_ => replyTo ! false)
          } else {
            Effect.persist(ObserverAdded(count, notifyTo)).thenRun(_ => replyTo ! true)
          }

        case SnapshotNow => Effect.persist(SnapshotMade)

        case GetSequenceNumber(replyTo) =>
          Effect.none.thenRun(_ => replyTo ! EventSourcedBehavior.lastSequenceNumber(context))
      }
    }

    private[scaladsl] def applyEvent(state: State, evt: Event): State =
      evt match {
        case DomainEvent =>
          val nextDomainEvtCount = state.domainEvtCount + 1

          if (nextDomainEvtCount < state.nextNotifyAt) state.copy(domainEvtCount = nextDomainEvtCount)
          else {
            import scala.collection.mutable

            val (nextNNA, nextNotifyAfter) = {
              var lowestNotifyAt = Int.MaxValue
              val inProgress = mutable.Map.empty[Int, RecipientRef[Done]]

              state.notifyAfter.keysIterator.foreach { at =>
                if (at > nextDomainEvtCount) {
                  lowestNotifyAt = lowestNotifyAt.min(at)
                  inProgress += (at -> state.notifyAfter(at))
                }
              // else ()
              }

              lowestNotifyAt -> inProgress.toMap
            }

            state.copy(domainEvtCount = nextDomainEvtCount, notifyAfter = nextNotifyAfter, nextNotifyAt = nextNNA)
          }

        case SnapshotMade => state
        case ObserverAdded(count, notifyTo) =>
          val nextNNA = state.nextNotifyAt.min(count)
          val nextNotifyAfter = state.notifyAfter.updated(count, notifyTo)

          state.copy(notifyAfter = nextNotifyAfter, nextNotifyAt = nextNNA)
      }
  }
}

class UnpersistentEventSourcedSpec extends AnyWordSpec with Matchers {
  import UnpersistentEventSourcedSpec._
  import org.slf4j.event.Level

  import akka.actor.testkit.typed.scaladsl._

  "Unpersistent EventSourcedBehavior" must {
    "generate a failing behavior from a non-EventSourcedBehavior" in {
      val notEventSourced =
        Behaviors.receive[Any] { (context, msg) =>
          context.log.info("Got message {}", msg)
          Behaviors.same
        }

      val unpersistent = UnpersistentBehavior.fromEventSourced[Any, Any, Any](notEventSourced)
      an[AssertionError] shouldBe thrownBy { unpersistent.behaviorTestKit }
      an[AssertionError] shouldBe thrownBy { unpersistent.eventProbe.extract() }
      an[AssertionError] shouldBe thrownBy { unpersistent.snapshotProbe.extract() }
    }

    "generate a Behavior from an EventSourcedBehavior and process RecoveryCompleted" in {
      import BehaviorUnderTest._

      val recoveryDone = TestInbox[Done]()
      val behavior = BehaviorUnderTest("test-1", recoveryDone.ref)

      // accessor API
      val unpersistent = UnpersistentBehavior.fromEventSourced[Command, Event, State](behavior)
      val testkit = unpersistent.behaviorTestKit
      val eventProbe = unpersistent.eventProbe
      val snapshotProbe = unpersistent.snapshotProbe

      assert(!eventProbe.hasEffects, "should not be events")
      assert(!snapshotProbe.hasEffects, "should not be snapshots")
      recoveryDone.expectMessage(Done)
      val logs = testkit.logEntries()
      logs.size shouldBe 1
      logs.head.level shouldBe Level.DEBUG
      logs.head.message shouldBe s"Recovered state for id [test-1] is [${State(0, Map.empty, Int.MaxValue)}]"
    }

    "publish events and evolve observed state in response to commands" in {
      import BehaviorUnderTest._

      val behavior = BehaviorUnderTest("test-1", TestInbox[Done]().ref)
      val replyTo = TestInbox[Done]()

      // resource-style API
      UnpersistentBehavior.fromEventSourced[Command, Event, State](behavior) { (testkit, eventProbe, snapshotProbe) =>
        testkit.clearLog()

        testkit.run(PersistDomainEvent(replyTo.ref))
        replyTo.expectMessage(Done)
        eventProbe.expectPersisted(DomainEvent, Set("domain"))
        snapshotProbe.drain() shouldBe empty
        assert(!testkit.hasEffects(), "should have no effects")
        testkit.clearLog()

        testkit.run(SnapshotNow)
        assert(!replyTo.hasMessages, "should not be a reply")

        val PersistenceEffect(_, seqNr, tags) = eventProbe.expectPersistedType[SnapshotMade.type]()
        seqNr shouldBe 2
        tags shouldBe empty

        snapshotProbe.expectPersisted(State(1, Map.empty, Int.MaxValue))
      }
    }

    "not publish events if the event handler fails" in {
      val behavior =
        EventSourcedBehavior[Int, Int, Unit](
          persistenceId = PersistenceId.ofUniqueId("degenerator"),
          emptyState = (),
          commandHandler = { (_, cmd: Int) =>
            cmd match {
              case nothing if nothing < 1 => Effect.none
              case 1                      => Effect.persist(1)
              case n                      => Effect.persist(Range(n, 0, -1)) // count down to 1
            }
          },
          eventHandler = { (_, evt) =>
            if (evt == 1) throw new RuntimeException("Kaboom!")
          })

      UnpersistentBehavior.fromEventSourced[Int, Int, Unit](behavior) { (testkit, eventProbe, _) =>
        val oneException = the[RuntimeException] thrownBy testkit.run(1)
        oneException.getMessage shouldBe "Kaboom!"
        eventProbe.hasEffects shouldBe false

        val twoException = the[RuntimeException] thrownBy testkit.run(2)
        twoException.getMessage shouldBe "Kaboom!"
        eventProbe.hasEffects shouldBe false
      }
    }

    "allow a state and starting offset to be injected" in {
      import BehaviorUnderTest._

      val recoveryDone = TestInbox[Done]()
      val behavior = BehaviorUnderTest("test-1", recoveryDone.ref)

      val notify3 = TestInbox[Done]()
      val initialState =
        Seq(ObserverAdded(3, notify3.ref), SnapshotMade, DomainEvent, DomainEvent)
          .foldLeft(State(0, Map.empty, Int.MaxValue))(applyEvent _)

      UnpersistentBehavior.fromEventSourced[Command, Event, State](behavior, Some(initialState -> 41L)) {
        (testkit, eventProbe, snapshotProbe) =>
          recoveryDone.expectMessage(Done)
          val logs = testkit.logEntries()
          logs.size shouldBe 1
          logs.head.level shouldBe Level.DEBUG
          logs.head.message shouldBe s"Recovered state for id [test-1] is [$initialState]"
          eventProbe.drain() shouldBe empty
          snapshotProbe.drain() shouldBe empty
          assert(!notify3.hasMessages, "no messages should be sent to notify3")

          val replyTo = TestInbox[Done]()
          testkit.run(PersistDomainEventAfter(2, replyTo.ref))
          assert(!testkit.hasEffects(), "should be no testkit effects")
          eventProbe.extract() shouldBe PersistenceEffect(DomainEvent, 42, Set("domain"))
          snapshotProbe.expectPersisted(State(3, Map.empty, Int.MaxValue))

          notify3.expectMessage(Done)
          replyTo.expectMessage(Done)
      }
    }

    "stash and unstash properly" in {
      import BehaviorUnderTest._

      val behavior = BehaviorUnderTest("test-1", TestInbox[Done]().ref)

      UnpersistentBehavior.fromEventSourced[Command, Event, State](behavior, None) {
        (testkit, eventProbe, snapshotProbe) =>
          val replyTo1 = TestInbox[Done]()
          val pde = PersistDomainEvent(TestInbox[Done]().ref)

          // stashes
          testkit.run(PersistDomainEventAfter(1, replyTo1.ref))
          eventProbe.drain() shouldBe empty
          snapshotProbe.drain() shouldBe empty
          assert(!replyTo1.hasMessages, "have not persisted first domain event")

          // unstashes
          testkit.run(pde)
          replyTo1.expectMessage(Done)

          // unstash, but nothing in the stash
          testkit.run(pde)
          assert(!replyTo1.hasMessages, "should not send again")
      }
    }

    "retrieve sequence number properly" in {
      import BehaviorUnderTest._

      val behavior = BehaviorUnderTest("test-1", TestInbox[Done]().ref)

      val randomStartingOffset =
        scala.util.Random.nextLong() match {
          case Long.MinValue => Long.MaxValue
          case x if x < 0    => -x
          case x             => x
        }

      UnpersistentBehavior.fromEventSourced[Command, Event, State](behavior, Some(initialState -> randomStartingOffset)) {
        (testkit, eventProbe, snapshotProbe) =>
          val replyTo = TestInbox[Long]()

          testkit.run(GetSequenceNumber(replyTo.ref))
          eventProbe.drain() shouldBe empty
          snapshotProbe.drain() shouldBe empty
          replyTo.expectMessage(randomStartingOffset)
      }
    }
  }
}
