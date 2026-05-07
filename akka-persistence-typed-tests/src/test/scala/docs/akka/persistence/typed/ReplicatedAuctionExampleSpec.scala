/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import java.time.Instant

import scala.concurrent.duration._

import docs.akka.persistence.typed.ReplicatedAuctionExampleSpec.AuctionEntity
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.persistence.typed.scaladsl.ReplicationContext
import akka.serialization.jackson.CborSerializable

object ReplicatedAuctionExampleSpec {

  //#setup
  object AuctionEntity {

    //#setup

    //#commands
    type MoneyAmount = Int

    case class Bid(bidder: String, offer: MoneyAmount, timestamp: Instant, originReplica: ReplicaId)

    sealed trait Command extends CborSerializable
    case object Finish extends Command // A timer needs to schedule this event at each replica
    final case class OfferBid(bidder: String, offer: MoneyAmount) extends Command
    final case class GetHighestBid(replyTo: ActorRef[Bid]) extends Command
    final case class IsClosed(replyTo: ActorRef[Boolean]) extends Command
    private case object Close extends Command // Internal, should not be sent from the outside
    //#commands

    //#events
    sealed trait Event extends CborSerializable
    final case class BidRegistered(bid: Bid) extends Event
    final case class AuctionFinished(atReplica: ReplicaId) extends Event
    final case class WinnerDecided(atReplica: ReplicaId, winningBid: Bid, highestCounterOffer: MoneyAmount)
        extends Event
    //#events

    //#phase
    /**
     * The auction passes through several workflow phases.
     * First, in `Running` `OfferBid` commands are accepted.
     *
     * `AuctionEntity` instances in all DCs schedule a `Finish` command
     * at a given time. That persists the `AuctionFinished` event and the
     * phase is in `Closing` until the auction is finished in all DCs.
     *
     * When the auction has been finished no more `OfferBid` commands are accepted.
     *
     * The auction is also finished immediately if `AuctionFinished` event from another
     * DC is seen before the scheduled `Finish` command. In that way the auction is finished
     * as quickly as possible in all DCs even though there might be some clock skew.
     *
     * One DC is responsible for finally deciding the winner and publishing the result.
     * All events must be collected from all DC before that can happen.
     * When the responsible DC has seen all `AuctionFinished` events from other DCs
     * all other events have also been propagated and it can persist `WinnerDecided` and
     * the auction is finally `Closed`.
     *
     */
    sealed trait AuctionPhase
    case object Running extends AuctionPhase
    final case class Closing(finishedAtReplica: Set[ReplicaId]) extends AuctionPhase
    case object Closed extends AuctionPhase
    //#phase

    //#state
    case class AuctionState(phase: AuctionPhase, highestBid: Bid, highestCounterOffer: MoneyAmount)
        extends CborSerializable {

      def applyEvent(event: Event): AuctionState =
        event match {
          case BidRegistered(b) =>
            if (isHigherBid(b, highestBid))
              withNewHighestBid(b)
            else
              withTooLowBid(b)
          case AuctionFinished(atDc) =>
            phase match {
              case Running =>
                copy(phase = Closing(Set(atDc)))
              case Closing(alreadyFinishedDcs) =>
                copy(phase = Closing(alreadyFinishedDcs + atDc))
              case _ =>
                this
            }
          case _: WinnerDecided =>
            copy(phase = Closed)
        }

      def withNewHighestBid(bid: Bid): AuctionState = {
        require(phase != Closed)
        require(isHigherBid(bid, highestBid))
        copy(highestBid = bid, highestCounterOffer = highestBid.offer // keep last highest bid around
        )
      }

      def withTooLowBid(bid: Bid): AuctionState = {
        require(phase != Closed)
        require(isHigherBid(highestBid, bid))
        copy(highestCounterOffer = highestCounterOffer.max(bid.offer)) // update highest counter offer
      }

      def isHigherBid(first: Bid, second: Bid): Boolean =
        first.offer > second.offer ||
        (first.offer == second.offer && first.timestamp.isBefore(second.timestamp)) || // if equal, first one wins
        // If timestamps are equal, choose by dc where the offer was submitted
        // In real auctions, this last comparison should be deterministic but unpredictable, so that submitting to a
        // particular DC would not be an advantage.
        (first.offer == second.offer && first.timestamp.equals(second.timestamp) && first.originReplica.id
          .compareTo(second.originReplica.id) < 0)
    }
    //#state

    //#setup
    def apply(
        replica: ReplicaId,
        name: String,
        initialBid: AuctionEntity.Bid, // the initial bid is basically the minimum price bidden at start time by the owner
        closingAt: Instant,
        responsibleForClosing: Boolean,
        allReplicas: Set[ReplicaId]): Behavior[Command] = Behaviors.setup[Command] { ctx =>
      Behaviors.withTimers { timers =>
        ReplicatedEventSourcing.commonJournalConfig(
          ReplicationId("auction", name, replica),
          allReplicas,
          PersistenceTestKitReadJournal.Identifier) { replicationCtx =>
          new AuctionEntity(ctx, replicationCtx, timers, closingAt, responsibleForClosing, allReplicas)
            .behavior(initialBid)
        }
      }
    }

  }

  class AuctionEntity(
      context: ActorContext[AuctionEntity.Command],
      replicationContext: ReplicationContext,
      timers: TimerScheduler[AuctionEntity.Command],
      closingAt: Instant,
      responsibleForClosing: Boolean,
      allReplicas: Set[ReplicaId]) {
    import AuctionEntity._

    private def behavior(initialBid: AuctionEntity.Bid): EventSourcedBehavior[Command, Event, AuctionState] =
      EventSourcedBehavior(
        replicationContext.persistenceId,
        AuctionState(phase = Running, highestBid = initialBid, highestCounterOffer = initialBid.offer),
        commandHandler,
        eventHandler).receiveSignal {
        case (state, RecoveryCompleted) => recoveryCompleted(state)
      }

    private def recoveryCompleted(state: AuctionState): Unit = {
      if (shouldClose(state))
        context.self ! Close

      val millisUntilClosing = closingAt.toEpochMilli - replicationContext.currentTimeMillis()
      timers.startSingleTimer(Finish, millisUntilClosing.millis)
    }
    //#setup

    //#command-handler
    def commandHandler(state: AuctionState, command: Command): Effect[Event, AuctionState] = {
      state.phase match {
        case Closing(_) | Closed =>
          command match {
            case GetHighestBid(replyTo) =>
              replyTo ! state.highestBid.copy(offer = state.highestCounterOffer) // TODO this is not as described
              Effect.none
            case IsClosed(replyTo) =>
              replyTo ! (state.phase == Closed)
              Effect.none
            case Finish =>
              context.log.info("Finish")
              Effect.persist(AuctionFinished(replicationContext.replicaId))
            case Close =>
              context.log.info("Close")
              require(shouldClose(state))
              Effect.persist(WinnerDecided(replicationContext.replicaId, state.highestBid, state.highestCounterOffer))
            case _: OfferBid =>
              // auction finished, no more bids accepted
              Effect.unhandled
          }
        case Running =>
          command match {
            case OfferBid(bidder, offer) =>
              Effect.persist(
                BidRegistered(
                  Bid(
                    bidder,
                    offer,
                    Instant.ofEpochMilli(replicationContext.currentTimeMillis()),
                    replicationContext.replicaId)))
            case GetHighestBid(replyTo) =>
              replyTo ! state.highestBid
              Effect.none
            case Finish =>
              Effect.persist(AuctionFinished(replicationContext.replicaId))
            case Close =>
              context.log.warn("Premature close")
              // Close should only be triggered when we have already finished
              Effect.unhandled
            case IsClosed(replyTo) =>
              replyTo ! false
              Effect.none
          }
      }
    }
    //#command-handler

    //#event-handler
    def eventHandler(state: AuctionState, event: Event): AuctionState = {

      val newState = state.applyEvent(event)
      context.log.info("Applying event {}. New start {}", event, newState)
      if (!replicationContext.recoveryRunning) {
        eventTriggers(event, newState)
      }
      newState

    }

    //#event-handler

    //#event-triggers
    private def eventTriggers(event: Event, newState: AuctionState): Unit = {
      event match {
        case finished: AuctionFinished =>
          newState.phase match {
            case Closing(alreadyFinishedAtDc) =>
              context.log.info(
                "AuctionFinished at {}, already finished at [{}]",
                finished.atReplica,
                alreadyFinishedAtDc.mkString(", "))
              if (alreadyFinishedAtDc(replicationContext.replicaId)) {
                if (shouldClose(newState)) context.self ! Close
              } else {
                context.log.info("Sending finish to self")
                context.self ! Finish
              }

            case _ => // no trigger for this state
          }
        case _ => // no trigger for this event
      }
    }

    private def shouldClose(state: AuctionState): Boolean = {
      responsibleForClosing && (state.phase match {
        case Closing(alreadyFinishedAtDc) =>
          val allDone = allReplicas.diff(alreadyFinishedAtDc).isEmpty
          if (!allDone) {
            context.log.info(
              s"Not closing auction as not all DCs have reported finished. All DCs: {}. Reported finished {}",
              allReplicas,
              alreadyFinishedAtDc)
          }
          allDone
        case _ =>
          false
      })
    }
    //#event-triggers

    //#setup
  }
  //#setup
}

class ReplicatedAuctionExampleSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ReplicatedAuctionExampleSpec.AuctionEntity._

  "Auction example" should {

    "work" in {
      val Replicas = Set(ReplicaId("DC-A"), ReplicaId("DC-B"))
      val auctionName = "old-skis"
      val initialBid = Bid("chbatey", 12, Instant.now(), ReplicaId("DC-A"))
      val closingAt = Instant.now().plusSeconds(10)

      val dcAReplica: ActorRef[Command] = spawn(
        AuctionEntity(ReplicaId("DC-A"), auctionName, initialBid, closingAt, responsibleForClosing = true, Replicas))
      val dcBReplica: ActorRef[Command] = spawn(
        AuctionEntity(ReplicaId("DC-B"), auctionName, initialBid, closingAt, responsibleForClosing = false, Replicas))

      dcAReplica ! OfferBid("me", 100)
      dcAReplica ! OfferBid("me", 99)
      dcAReplica ! OfferBid("me", 202)

      eventually {
        val replyProbe = createTestProbe[Bid]()
        dcAReplica ! GetHighestBid(replyProbe.ref)
        val bid = replyProbe.expectMessageType[Bid]
        bid.offer shouldEqual 202
      }

      dcAReplica ! Finish
      eventually {
        val finishProbe = createTestProbe[Boolean]()
        dcAReplica ! IsClosed(finishProbe.ref)
        finishProbe.expectMessage(true)
      }
      eventually {
        val finishProbe = createTestProbe[Boolean]()
        dcBReplica ! IsClosed(finishProbe.ref)
        finishProbe.expectMessage(true)
      }
    }
  }
}
