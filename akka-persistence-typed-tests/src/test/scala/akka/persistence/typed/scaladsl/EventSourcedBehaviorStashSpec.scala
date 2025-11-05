/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.NotUsed
import akka.actor.Dropped
import akka.actor.UnhandledMessage
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.javadsl.StashOverflowException
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted

object EventSourcedBehaviorStashSpec {
  def conf: Config = ConfigFactory.parseString(s"""
    #akka.loglevel = DEBUG
    #akka.persistence.typed.log-stashing = on
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.journal.plugin = "failure-journal"
    # tune it down a bit so we can hit limit
    akka.persistence.typed.stash-capacity = 500
    failure-journal = $${akka.persistence.journal.inmem}
    failure-journal {
      class = "akka.persistence.typed.scaladsl.ChaosJournal"
    }
    """).withFallback(ConfigFactory.defaultReference()).resolve()

  sealed trait Command[ReplyMessage]
  // Unstash and change to active mode
  final case class Activate(id: String, val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Change to active mode, stash incoming Increment
  final case class Deactivate(id: String, val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Persist Incremented if in active mode, otherwise stashed
  final case class Increment(id: String, val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Persist ValueUpdated, independent of active/inactive
  final case class UpdateValue(id: String, value: Int, val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Retrieve current state, independent of active/inactive
  final case class GetValue(replyTo: ActorRef[State]) extends Command[State]
  final case class Unhandled(replyTo: ActorRef[NotUsed]) extends Command[NotUsed]
  final case class Throw(id: String, t: Throwable, val replyTo: ActorRef[Ack]) extends Command[Ack]
  final case class IncrementThenThrow(id: String, t: Throwable) extends Command[Ack]
  final case class Slow(id: String, latch: CountDownLatch, val replyTo: ActorRef[Ack]) extends Command[Ack]

  final case class Ack(id: String)

  sealed trait Event
  final case class Incremented(delta: Int) extends Event
  final case class ValueUpdated(value: Int) extends Event
  case object Activated extends Event
  case object Deactivated extends Event

  final case class State(value: Int, active: Boolean)

  def counter(persistenceId: PersistenceId, signalProbe: Option[ActorRef[String]] = None): Behavior[Command[_]] =
    Behaviors
      .supervise[Command[_]] {
        Behaviors.setup(_ => eventSourcedCounter(persistenceId, signalProbe))
      }
      .onFailure(SupervisorStrategy.restart.withLoggingEnabled(enabled = false))

  def eventSourcedCounter(
      persistenceId: PersistenceId,
      signalProbe: Option[ActorRef[String]]): EventSourcedBehavior[Command[_], Event, State] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command[_], Event, State](
        persistenceId,
        emptyState = State(0, active = true),
        commandHandler = (state, command) => {
          if (state.active) active(state, command)
          else inactive(state, command)
        },
        eventHandler = (state, evt) =>
          evt match {
            case Incremented(delta) =>
              if (!state.active) throw new IllegalStateException
              State(state.value + delta, active = true)
            case ValueUpdated(value) =>
              State(value, active = state.active)
            case Activated =>
              if (state.active) throw new IllegalStateException
              state.copy(active = true)
            case Deactivated =>
              if (!state.active) throw new IllegalStateException
              state.copy(active = false)
          })
      .onPersistFailure(SupervisorStrategy
        .restartWithBackoff(1.second, maxBackoff = 2.seconds, 0.0)
        .withLoggingEnabled(enabled = false))
      .receiveSignal {
        case (state, RecoveryCompleted) => signalProbe.foreach(_ ! s"RecoveryCompleted-${state.value}")
        case (_, PostStop)              => signalProbe.foreach(_ ! "PostStop")
      }
  }

  private def active(state: State, command: Command[_]): ReplyEffect[Event, State] = {
    command match {
      case Increment(id, replyTo) =>
        Effect.persist(Incremented(1)).thenReply(replyTo)(_ => Ack(id))
      case UpdateValue(id, value, replyTo) =>
        Effect.persist(ValueUpdated(value)).thenReply(replyTo)(_ => Ack(id))
      case GetValue(replyTo) =>
        Effect.reply(replyTo)(state)
      case Deactivate(id, replyTo) =>
        Effect.persist(Deactivated).thenReply(replyTo)(_ => Ack(id))
      case Activate(id, replyTo) =>
        // already active
        Effect.reply(replyTo)(Ack(id))
      case _: Unhandled =>
        Effect.unhandled.thenNoReply()
      case Throw(id, t, replyTo) =>
        replyTo ! Ack(id)
        throw t
      case IncrementThenThrow(_, throwable) =>
        Effect.persist(Incremented(1)).thenRun((_: State) => throw throwable).thenNoReply()
      case Slow(id, latch, replyTo) =>
        latch.await(30, TimeUnit.SECONDS)
        Effect.reply(replyTo)(Ack(id))
    }
  }

  private def inactive(state: State, command: Command[_]): ReplyEffect[Event, State] = {
    command match {
      case _: Increment =>
        Effect.stash()
      case UpdateValue(id, value, replyTo) =>
        Effect.persist(ValueUpdated(value)).thenReply(replyTo)(_ => Ack(id))
      case GetValue(replyTo) =>
        Effect.reply(replyTo)(state)
      case Deactivate(id, replyTo) =>
        // already inactive
        Effect.reply(replyTo)(Ack(id))
      case Activate(id, replyTo) =>
        Effect.persist(Activated).thenReply(replyTo)((_: State) => Ack(id)).thenUnstashAll()
      case _: Unhandled =>
        Effect.unhandled.thenNoReply()
      case Throw(id, t, replyTo) =>
        replyTo ! Ack(id)
        throw t
      case _: IncrementThenThrow =>
        Effect.stash()
      case _: Slow =>
        Effect.stash()
    }
  }
}

class EventSourcedBehaviorStashSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorStashSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedBehaviorStashSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()}")

  "A typed persistent actor that is stashing commands" must {

    "stash and unstash" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()

      c ! Increment("1", ackProbe.ref)
      ackProbe.expectMessage(Ack("1"))

      c ! Deactivate("2", ackProbe.ref)
      ackProbe.expectMessage(Ack("2"))

      c ! Increment("3", ackProbe.ref)
      c ! Increment("4", ackProbe.ref)
      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(1, active = false))

      c ! Activate("5", ackProbe.ref)
      c ! Increment("6", ackProbe.ref)

      ackProbe.expectMessage(Ack("5"))
      ackProbe.expectMessage(Ack("3"))
      ackProbe.expectMessage(Ack("4"))
      ackProbe.expectMessage(Ack("6"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(4, active = true))
    }

    "handle mix of stash, persist and unstash" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()

      c ! Increment("1", ackProbe.ref)
      ackProbe.expectMessage(Ack("1"))

      c ! Deactivate("2", ackProbe.ref)
      ackProbe.expectMessage(Ack("2"))

      c ! Increment("3", ackProbe.ref)
      c ! Increment("4", ackProbe.ref)
      // UpdateValue will persist when inactive (with previously stashed commands)
      c ! UpdateValue("5", 100, ackProbe.ref)
      ackProbe.expectMessage(Ack("5"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(100, active = false))

      c ! Activate("6", ackProbe.ref)
      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(102, active = true))
      ackProbe.expectMessage(Ack("6"))
      ackProbe.expectMessage(Ack("3"))
      ackProbe.expectMessage(Ack("4"))
    }

    "unstash in right order" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()

      c ! Increment(s"inc-1", ackProbe.ref)

      c ! Deactivate(s"deact", ackProbe.ref)

      c ! Increment(s"inc-2", ackProbe.ref)
      c ! Increment(s"inc-3", ackProbe.ref)
      c ! Increment(s"inc-4", ackProbe.ref)

      c ! Activate(s"act", ackProbe.ref)

      c ! Increment(s"inc-5", ackProbe.ref)
      c ! Increment(s"inc-6", ackProbe.ref)
      c ! Increment(s"inc-7", ackProbe.ref)

      c ! GetValue(stateProbe.ref)
      val finalState = stateProbe.expectMessageType[State](5.seconds)

      // verify the order

      ackProbe.expectMessage(Ack("inc-1"))
      ackProbe.expectMessage(Ack("deact"))
      ackProbe.expectMessage(Ack("act"))
      ackProbe.expectMessage(Ack("inc-2"))
      ackProbe.expectMessage(Ack("inc-3"))
      ackProbe.expectMessage(Ack("inc-4"))
      ackProbe.expectMessage(Ack("inc-5"))
      ackProbe.expectMessage(Ack("inc-6"))
      ackProbe.expectMessage(Ack("inc-7"))

      finalState.value should ===(7)
    }

    "handle many stashed" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()
      val notUsedProbe = TestProbe[NotUsed]()
      val unhandledProbe = createTestProbe[UnhandledMessage]()
      system.eventStream ! EventStream.Subscribe(unhandledProbe.ref)

      (1 to 100).foreach { n =>
        c ! Increment(s"inc-1-$n", ackProbe.ref)
      }

      (1 to 3).foreach { n =>
        c ! Deactivate(s"deact-2-$n", ackProbe.ref)
      }

      (1 to 100).foreach { n =>
        if (n % 10 == 0)
          c ! Unhandled(notUsedProbe.ref)
        c ! Increment(s"inc-3-$n", ackProbe.ref)
      }

      c ! GetValue(stateProbe.ref)

      (1 to 5).foreach { n =>
        c ! UpdateValue(s"upd-4-$n", n * 1000, ackProbe.ref)
      }

      (1 to 3).foreach { n =>
        c ! Activate(s"act-5-$n", ackProbe.ref)
      }

      (1 to 100).foreach { n =>
        c ! Increment(s"inc-6-$n", ackProbe.ref)
      }

      c ! GetValue(stateProbe.ref)

      (6 to 8).foreach { n =>
        c ! UpdateValue(s"upd-7-$n", n * 1000, ackProbe.ref)
      }

      (1 to 3).foreach { n =>
        c ! Deactivate(s"deact-8-$n", ackProbe.ref)
      }

      (1 to 100).foreach { n =>
        c ! Increment(s"inc-9-$n", ackProbe.ref)
      }

      (1 to 3).foreach { n =>
        c ! Activate(s"act-10-$n", ackProbe.ref)
      }

      c ! GetValue(stateProbe.ref)

      unhandledProbe.receiveMessages(10)

      // a lot of events, so give it some extra time to complete
      val value1 = stateProbe.expectMessageType[State](10.seconds).value
      val value2 = stateProbe.expectMessageType[State](10.seconds).value
      val value3 = stateProbe.expectMessageType[State](10.seconds).value

      // verify the order

      (1 to 100).foreach { n =>
        ackProbe.expectMessage(Ack(s"inc-1-$n"))
      }

      (1 to 3).foreach { n =>
        ackProbe.expectMessage(Ack(s"deact-2-$n"))
      }

      (1 to 5).foreach { n =>
        ackProbe.expectMessage(Ack(s"upd-4-$n"))
      }

      ackProbe.expectMessage(Ack("act-5-1"))

      (1 to 100).foreach { n =>
        ackProbe.expectMessage(Ack(s"inc-3-$n"))
      }

      (2 to 3).foreach { n =>
        ackProbe.expectMessage(Ack(s"act-5-$n"))
      }

      (1 to 100).foreach { n =>
        ackProbe.expectMessage(Ack(s"inc-6-$n"))
      }

      (6 to 8).foreach { n =>
        ackProbe.expectMessage(Ack(s"upd-7-$n"))
      }

      (1 to 3).foreach { n =>
        ackProbe.expectMessage(Ack(s"deact-8-$n"))
      }

      ackProbe.expectMessage(Ack("act-10-1"))

      (1 to 100).foreach { n =>
        ackProbe.expectMessage(Ack(s"inc-9-$n"))
      }

      (2 to 3).foreach { n =>
        ackProbe.expectMessage(Ack(s"act-10-$n"))
      }

      value1 should ===(100)
      value2 should ===(5200)
      value3 should ===(8100)
    }

    "discard user stash when restarted due to thrown exception" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()

      c ! Increment("inc-1", ackProbe.ref)
      ackProbe.expectMessage(Ack("inc-1"))

      c ! Deactivate("deact", ackProbe.ref)
      ackProbe.expectMessage(Ack("deact"))

      c ! Increment("inc-2", ackProbe.ref)
      c ! Increment("inc-3", ackProbe.ref)
      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(1, active = false))

      c ! Throw("throw", new TestException("test"), ackProbe.ref)
      ackProbe.expectMessage(Ack("throw"))

      c ! Increment("inc-4", ackProbe.ref)
      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(1, active = false))

      c ! Activate("act", ackProbe.ref)
      c ! Increment("inc-5", ackProbe.ref)

      ackProbe.expectMessage(Ack("act"))
      // inc-2 an inc-3 was in user stash, and didn't survive restart, as expected
      ackProbe.expectMessage(Ack("inc-4"))
      ackProbe.expectMessage(Ack("inc-5"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(3, active = true))
    }

    "discard internal stash when restarted due to thrown exception" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()
      val latch = new CountDownLatch(1)

      // make first command slow to ensure that all subsequent commands are enqueued first
      c ! Slow("slow", latch, ackProbe.ref)

      (1 to 10).foreach { n =>
        if (n == 3)
          c ! IncrementThenThrow(s"inc-$n", new TestException("test"))
        else
          c ! Increment(s"inc-$n", ackProbe.ref)
      }

      latch.countDown()
      ackProbe.expectMessage(Ack("slow"))

      ackProbe.expectMessage(Ack("inc-1"))
      ackProbe.expectMessage(Ack("inc-2"))
      ackProbe.expectNoMessage()

      c ! Increment("inc-11", ackProbe.ref)
      ackProbe.expectMessage(Ack("inc-11"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(4, active = true))
    }

    "preserve internal stash when persist failed" in {
      val c = spawn(counter(PersistenceId.ofUniqueId("fail-fifth-a")))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()

      (1 to 10).foreach { n =>
        c ! Increment(s"inc-$n", ackProbe.ref)
      }

      (1 to 10).foreach { n =>
        if (n != 5)
          ackProbe.expectMessage(Ack(s"inc-$n"))
      }

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(9, active = true))
    }

    "unstash messages after stashed GetState" in {
      import akka.persistence.typed.internal.EventSourcedBehaviorImpl.{ GetState, GetStateReply }

      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()
      val getStateProbe = TestProbe[GetStateReply[State]]()
      val latch = new CountDownLatch(1)
      c ! Increment(s"inc-1", ackProbe.ref)
      ackProbe.expectMessage(Ack(s"inc-1"))
      // make sure all the following messages are already in the mailbox
      c ! Slow("slow", latch, ackProbe.ref)
      // this will persist an event
      c ! Increment(s"inc-2", ackProbe.ref)
      // while the next two messages are already in the mailbox, so they will be stashed in the persisting state
      c.unsafeUpcast[Any] ! GetState(getStateProbe.ref)
      c ! Increment(s"inc-3", ackProbe.ref)
      c ! Increment(s"inc-4", ackProbe.ref)

      latch.countDown()

      ackProbe.expectMessage(Ack("slow"))
      ackProbe.expectMessage(Ack(s"inc-2"))
      getStateProbe.expectMessageType[GetStateReply[State]]
      ackProbe.expectMessage(Ack(s"inc-3"))
      ackProbe.expectMessage(Ack(s"inc-4"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(4, active = true))
    }

    "preserve user stash when persist failed" in {
      val c = spawn(counter(PersistenceId.ofUniqueId("fail-fifth-b")))
      val ackProbe = TestProbe[Ack]()
      val stateProbe = TestProbe[State]()

      c ! Increment("inc-1", ackProbe.ref)
      ackProbe.expectMessage(Ack("inc-1"))

      c ! Deactivate("deact", ackProbe.ref)
      ackProbe.expectMessage(Ack("deact"))

      c ! Increment("inc-2", ackProbe.ref)
      c ! Increment("inc-3", ackProbe.ref) // this will fail when unstashed
      c ! Increment("inc-4", ackProbe.ref)
      c ! Increment("inc-5", ackProbe.ref)
      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(1, active = false))

      c ! Activate("act", ackProbe.ref)
      c ! Increment("inc-6", ackProbe.ref)

      ackProbe.expectMessage(Ack("act"))
      ackProbe.expectMessage(Ack("inc-2"))
      // inc-3 failed
      // inc-4, inc-5 still in user stash and processed due to UnstashAll that was in progress
      ackProbe.expectMessage(Ack("inc-4"))
      ackProbe.expectMessage(Ack("inc-5"))
      ackProbe.expectMessage(Ack("inc-6"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(5, active = true))
    }

    trait StashLimit {
      def customStashLimit: Option[Int]
      val probe = TestProbe[AnyRef]()
      system.toClassic.eventStream.subscribe(probe.ref.toClassic, classOf[Dropped])
      val behavior = Behaviors.setup[String] { context =>
        val esb = EventSourcedBehavior[String, String, Boolean](
          persistenceId = PersistenceId.ofUniqueId("stash-is-full-drop"),
          emptyState = false,
          commandHandler = { (state, command) =>
            state match {
              case false =>
                command match {
                  case "ping" =>
                    probe.ref ! "pong"
                    Effect.none
                  case "start-stashing" =>
                    Effect.persist("start-stashing").thenRun(_ => probe ! "started-stashing")
                  case msg =>
                    probe.ref ! msg
                    Effect.none
                }

              case true =>
                command match {
                  case "unstash" =>
                    Effect.persist("unstash").thenRun((_: Boolean) => context.self ! "done-unstashing").thenUnstashAll()
                  case _ =>
                    Effect.stash()
                }
            }
          }, {
            case (_, "start-stashing") => true
            case (_, "unstash")        => false
            case (_, _)                => throw new IllegalArgumentException()
          })
        customStashLimit match {
          case Some(value) => esb.withStashCapacity(value)
          case None        => esb
        }

      }
    }

    "discard when stash has reached limit with default dropped setting" in new StashLimit {
      override val customStashLimit: Option[Int] = None
      val c = spawn(behavior)

      // make sure it completed recovery, before we try to overfill the stash
      c ! "ping"
      probe.expectMessage("pong")

      c ! "start-stashing"
      // make sure write completes before sending more commands so that they are not auto-stashed by ESB
      probe.expectMessage("started-stashing")

      val limit = system.settings.config.getInt("akka.persistence.typed.stash-capacity")
      LoggingTestKit.warn("Stash buffer is full, dropping message").expect {
        (0 to limit).foreach { n =>
          c ! s"cmd-$n" // limit triggers overflow
        }
        probe.expectMessageType[Dropped]
      }

      // we can still unstash and continue interacting
      c ! "unstash"
      (0 until limit).foreach { n =>
        probe.expectMessage(s"cmd-$n")
      }
      probe.expectMessage("done-unstashing") // before actually unstashing, see above

      c ! "ping"
      probe.expectMessage("pong")
    }

    "discard when custom stash buffer has reached limit with default dropped setting" in new StashLimit {
      val customLimit = 100
      override val customStashLimit: Option[Int] = Some(customLimit)

      val c = spawn(behavior)

      // make sure it completed recovery, before we try to overfill the stash
      c ! "ping"
      probe.expectMessage("pong")

      c ! "start-stashing"
      // make sure write completes before sending more commands so that they are not auto-stashed by ESB
      probe.expectMessage("started-stashing")

      LoggingTestKit.warn("Stash buffer is full, dropping message").expect {
        (0 to customLimit).foreach { n =>
          c ! s"cmd-$n" // limit triggers overflow
        }
        probe.expectMessageType[Dropped]
      }

      // we can still unstash and continue interacting
      c ! "unstash"
      (0 until customLimit).foreach { n =>
        probe.expectMessage(s"cmd-$n")
      }

      probe.expectMessage("done-unstashing") // before actually unstashing, see above

      c ! "ping"
      probe.expectMessage("pong")
    }

    "fail when stash has reached limit if configured to fail" in {
      // persistence settings is system wide, so we need to have a custom testkit/actorsystem here
      val failStashTestKit = ActorTestKit(
        "EventSourcedBehaviorStashSpec-stash-overflow-fail",
        ConfigFactory
          .parseString("akka.persistence.typed.stash-overflow-strategy=fail")
          .withFallback(EventSourcedBehaviorStashSpec.conf))
      try {
        val probe = failStashTestKit.createTestProbe[AnyRef]()
        val behavior =
          EventSourcedBehavior[String, String, String](
            PersistenceId.ofUniqueId("stash-is-full-fail"),
            "",
            commandHandler = {
              case (_, "ping") =>
                probe.ref ! "pong"
                Effect.none
              case (_, _) =>
                Effect.stash()
            },
            (state, _) => state)

        val c = failStashTestKit.spawn(behavior)

        // make sure recovery completed
        c ! "ping"
        probe.expectMessage("pong")

        LoggingTestKit
          .error[StashOverflowException]
          .expect {
            val limit = system.settings.config.getInt("akka.persistence.typed.stash-capacity")
            (0 to limit).foreach { n =>
              c ! s"cmd-$n" // limit triggers overflow
            }
            probe.expectTerminated(c, 10.seconds)
          }(failStashTestKit.system)
      } finally {
        failStashTestKit.shutdownTestKit()
      }
    }

    "stop from PoisonPill even though user stash is not empty" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()

      c ! Increment("1", ackProbe.ref)
      ackProbe.expectMessage(Ack("1"))

      c ! Deactivate("2", ackProbe.ref)
      ackProbe.expectMessage(Ack("2"))

      // stash 3 and 4
      c ! Increment("3", ackProbe.ref)
      c ! Increment("4", ackProbe.ref)

      c.toClassic ! PoisonPill
      ackProbe.expectTerminated(c)
    }

    "stop from PoisonPill after unstashing completed" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]()

      c ! Increment("1", ackProbe.ref)
      ackProbe.expectMessage(Ack("1"))

      c ! Deactivate("2", ackProbe.ref)
      ackProbe.expectMessage(Ack("2"))

      // stash 3 and 4
      c ! Increment("3", ackProbe.ref)
      c ! Increment("4", ackProbe.ref)

      // start unstashing
      c ! Activate("5", ackProbe.ref)
      c.toClassic ! PoisonPill
      // 6 shouldn't make it, already stopped
      c ! Increment("6", ackProbe.ref)

      ackProbe.expectMessage(Ack("5"))
      // not stopped before 3 and 4 were processed
      ackProbe.expectMessage(Ack("3"))
      ackProbe.expectMessage(Ack("4"))

      ackProbe.expectTerminated(c)

      // 6 shouldn't make it, already stopped
      ackProbe.expectNoMessage(100.millis)
    }

    "stop from PoisonPill after recovery completed" in {
      val pid = nextPid()
      val c = spawn(counter(pid))
      val ackProbe = TestProbe[Ack]()

      c ! Increment("1", ackProbe.ref)
      c ! Increment("2", ackProbe.ref)
      c ! Increment("3", ackProbe.ref)
      ackProbe.expectMessage(Ack("1"))
      ackProbe.expectMessage(Ack("2"))
      ackProbe.expectMessage(Ack("3"))

      val signalProbe = TestProbe[String]()
      val c2 = spawn(counter(pid, Some(signalProbe.ref)))
      // this PoisonPill will most likely be received in RequestingRecoveryPermit since it's sent immediately
      c2.toClassic ! PoisonPill
      signalProbe.expectMessage("RecoveryCompleted-3")
      signalProbe.expectMessage("PostStop")

      val c3 = spawn(counter(pid, Some(signalProbe.ref)))
      // this PoisonPill will most likely be received in RequestingRecoveryPermit since it's sent slightly afterwards
      Thread.sleep(1)
      c3.toClassic ! PoisonPill
      c3 ! Increment("4", ackProbe.ref)
      signalProbe.expectMessage("RecoveryCompleted-3")
      signalProbe.expectMessage("PostStop")
      ackProbe.expectNoMessage(20.millis)

      val c4 = spawn(counter(pid, Some(signalProbe.ref)))
      signalProbe.expectMessage("RecoveryCompleted-3")
      // this PoisonPill will be received in Running
      c4.toClassic ! PoisonPill
      c4 ! Increment("4", ackProbe.ref)
      signalProbe.expectMessage("PostStop")
      ackProbe.expectNoMessage(20.millis)

      val c5 = spawn(counter(pid, Some(signalProbe.ref)))
      signalProbe.expectMessage("RecoveryCompleted-3")
      c5 ! Increment("4", ackProbe.ref)
      c5 ! Increment("5", ackProbe.ref)
      // this PoisonPill will most likely be received in PersistingEvents
      c5.toClassic ! PoisonPill
      c5 ! Increment("6", ackProbe.ref)
      ackProbe.expectMessage(Ack("4"))
      ackProbe.expectMessage(Ack("5"))
      signalProbe.expectMessage("PostStop")
      ackProbe.expectNoMessage(20.millis)
    }
  }

}
