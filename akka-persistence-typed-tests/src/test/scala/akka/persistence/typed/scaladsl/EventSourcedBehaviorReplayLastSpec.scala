/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin
import akka.persistence.typed.PersistenceId
import akka.serialization.jackson.CborSerializable

object EventSourcedBehaviorReplayLastSpec extends Matchers {

  sealed trait Command extends CborSerializable
  final case class Increment(id: String) extends Command
  final case class GetValue(replyTo: ActorRef[State]) extends Command

  sealed trait Event extends CborSerializable
  final case class Incremented(id: String) extends Event

  final case class State(value: Int, history: Vector[String]) extends CborSerializable

  def counter(
      @nowarn("msg=never used") ctx: ActorContext[Command],
      persistenceId: PersistenceId,
      probe: Option[ActorRef[(State, Event)]] = None): EventSourcedBehavior[Command, Event, State] = {
    EventSourcedBehavior[Command, Event, State](
      persistenceId,
      emptyState = State(0, Vector.empty),
      commandHandler = (state, cmd) =>
        cmd match {
          case Increment(id) =>
            Effect.persist(Incremented(id))

          case GetValue(replyTo) =>
            replyTo ! state
            Effect.none

        },
      eventHandler = (state, evt) =>
        evt match {
          case Incremented(id) =>
            probe.foreach(_ ! ((state, evt)))
            State(state.value + 1, state.history :+ id)
        })
  }.withRecovery(Recovery.replayOnlyLast)

}

class EventSourcedBehaviorReplayLastSpec
    extends ScalaTestWithActorTestKit(
      PersistenceTestKitPlugin.config.withFallback(PersistenceTestKitSnapshotPlugin.config))
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedBehaviorReplayLastSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()}")

  "EventSourcedBehavior with Recovery.replayOnlyLast" must {
    "recover from last event only" in {
      val pid = nextPid()
      val c = spawn(Behaviors.setup[Command](ctx => counter(ctx, pid)))
      val replyProbe = TestProbe[State]()

      c ! Increment("a")
      c ! Increment("b")
      c ! Increment("c")
      c ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, Vector("a", "b", "c")))
      testKit.stop(c)
      replyProbe.expectTerminated(c)

      val probe = TestProbe[(State, Event)]()
      val c2 = spawn(Behaviors.setup[Command](ctx => counter(ctx, pid, Some(probe.ref))))
      // replayed the last event
      probe.expectMessage((State(0, Vector.empty), Incremented("c")))
      probe.expectNoMessage()

      c2 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(1, Vector("c")))

      c2 ! Increment("d")
      c2 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(2, Vector("c", "d")))
    }

  }
}
