/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.persistence.testkit.PersistenceTestKitPlugin;
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.RecoveryCompleted;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.ReplicationId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.CommandHandlerBuilder;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior;
import akka.persistence.typed.javadsl.ReplicatedEventSourcing;
import akka.persistence.typed.javadsl.ReplicationContext;
import akka.persistence.typed.javadsl.SignalHandler;
import akka.serialization.jackson.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

// format: OFF
import static jdocs.akka.persistence.typed.AuctionEntity.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
// format: ON

public class ReplicatedAuctionExampleTest extends JUnitSuite {
  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(PersistenceTestKitPlugin.getInstance().config());

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void auctionExample() {
    String auctionName = "old-skis";
    Bid initialBid = new Bid("chbatey", 12, Instant.now(), R1);
    Instant closeAt = Instant.now().plusSeconds(10);

    ActorRef<Command> replicaA =
        testKit.spawn(AuctionEntity.create(R1, auctionName, initialBid, closeAt, true));
    ActorRef<Command> replicaB =
        testKit.spawn(AuctionEntity.create(R2, auctionName, initialBid, closeAt, false));

    replicaA.tell(new OfferBid("me", 100));
    replicaA.tell(new OfferBid("me", 99));
    replicaA.tell(new OfferBid("me", 202));

    TestProbe<Bid> replyProbe = testKit.createTestProbe();
    replyProbe.awaitAssert(
        () -> {
          replicaA.tell(new GetHighestBid(replyProbe.ref()));
          Bid bid = replyProbe.expectMessageClass(Bid.class);
          assertEquals(202, bid.offer);
          return bid;
        });

    replicaA.tell(Finish.INSTANCE);

    TestProbe<Boolean> finishProbe = testKit.createTestProbe();
    finishProbe.awaitAssert(
        () -> {
          replicaA.tell(new IsClosed(finishProbe.ref()));
          return finishProbe.expectMessage(true);
        });
    finishProbe.awaitAssert(
        () -> {
          replicaB.tell(new IsClosed(finishProbe.ref()));
          return finishProbe.expectMessage(true);
        });
  }
}

// #setup
class AuctionEntity
    extends ReplicatedEventSourcedBehavior<
        AuctionEntity.Command, AuctionEntity.Event, AuctionEntity.AuctionState> {

  public static ReplicaId R1 = new ReplicaId("R1");
  public static ReplicaId R2 = new ReplicaId("R2");

  public static Set<ReplicaId> ALL_REPLICAS = new HashSet<>(Arrays.asList(R1, R2));

  private final ActorContext<Command> context;
  private final TimerScheduler<Command> timers;
  private final Bid initialBid;
  private final Instant closingAt;
  private final boolean responsibleForClosing;

  // #setup

  // #commands
  public static final class Bid {
    public final String bidder;
    public final int offer;
    public final Instant timestamp;
    public final ReplicaId originReplica;

    public Bid(String bidder, int offer, Instant timestamp, ReplicaId originReplica) {
      this.bidder = bidder;
      this.offer = offer;
      this.timestamp = timestamp;
      this.originReplica = originReplica;
    }
  }

  interface Command extends CborSerializable {}

  public enum Finish implements Command {
    INSTANCE
  }

  public static final class OfferBid implements Command {
    public final String bidder;
    public final int offer;

    public OfferBid(String bidder, int offer) {
      this.bidder = bidder;
      this.offer = offer;
    }
  }

  public static final class GetHighestBid implements Command {
    public final ActorRef<Bid> replyTo;

    public GetHighestBid(ActorRef<Bid> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static final class IsClosed implements Command {
    public final ActorRef<Boolean> replyTo;

    public IsClosed(ActorRef<Boolean> replyTo) {
      this.replyTo = replyTo;
    }
  }

  private enum Close implements Command {
    INSTANCE
  }
  // #commands

  // #events
  interface Event extends CborSerializable {}

  public static final class BidRegistered implements Event {
    public final Bid bid;

    @JsonCreator
    public BidRegistered(Bid bid) {
      this.bid = bid;
    }
  }

  public static final class AuctionFinished implements Event {
    public final ReplicaId atReplica;

    @JsonCreator
    public AuctionFinished(ReplicaId atReplica) {
      this.atReplica = atReplica;
    }
  }

  public static final class WinnerDecided implements Event {
    public final ReplicaId atReplica;
    public final Bid winningBid;
    public final int amount;

    public WinnerDecided(ReplicaId atReplica, Bid winningBid, int amount) {
      this.atReplica = atReplica;
      this.winningBid = winningBid;
      this.amount = amount;
    }
  }
  // #events

  // #state
  static class AuctionState implements CborSerializable {

    final boolean stillRunning;
    final Bid highestBid;
    final int highestCounterOffer;
    final Set<String> finishedAtDc;

    AuctionState(
        boolean stillRunning, Bid highestBid, int highestCounterOffer, Set<String> finishedAtDc) {
      this.stillRunning = stillRunning;
      this.highestBid = highestBid;
      this.highestCounterOffer = highestCounterOffer;
      this.finishedAtDc = finishedAtDc;
    }

    AuctionState withNewHighestBid(Bid bid) {
      assertTrue(stillRunning);
      assertTrue(isHigherBid(bid, highestBid));
      return new AuctionState(
          stillRunning, bid, highestBid.offer, finishedAtDc); // keep last highest bid around
    }

    AuctionState withTooLowBid(Bid bid) {
      assertTrue(stillRunning);
      assertTrue(isHigherBid(highestBid, bid));
      return new AuctionState(
          stillRunning, highestBid, Math.max(highestCounterOffer, bid.offer), finishedAtDc);
    }

    static Boolean isHigherBid(Bid first, Bid second) {
      return first.offer > second.offer
          || (first.offer == second.offer && first.timestamp.isBefore(second.timestamp))
          || // if equal, first one wins
          // If timestamps are equal, choose by dc where the offer was submitted
          // In real auctions, this last comparison should be deterministic but unpredictable, so
          // that submitting to a
          // particular DC would not be an advantage.
          (first.offer == second.offer
              && first.timestamp.equals(second.timestamp)
              && first.originReplica.id().compareTo(second.originReplica.id()) < 0);
    }

    AuctionState addFinishedAtReplica(String replica) {
      Set<String> s = new HashSet<>(finishedAtDc);
      s.add(replica);
      return new AuctionState(
          false, highestBid, highestCounterOffer, Collections.unmodifiableSet(s));
    }

    public AuctionState close() {
      return new AuctionState(false, highestBid, highestCounterOffer, Collections.emptySet());
    }

    public boolean isClosed() {
      return !stillRunning && finishedAtDc.isEmpty();
    }
  }
  // #state

  // #setup
  public static Behavior<Command> create(
      ReplicaId replica,
      String name,
      Bid initialBid,
      Instant closingAt,
      boolean responsibleForClosing) {
    return Behaviors.setup(
        ctx ->
            Behaviors.withTimers(
                timers ->
                    ReplicatedEventSourcing.commonJournalConfig(
                        new ReplicationId("Auction", name, replica),
                        ALL_REPLICAS,
                        PersistenceTestKitReadJournal.Identifier(),
                        replicationCtx ->
                            new AuctionEntity(
                                ctx,
                                replicationCtx,
                                timers,
                                initialBid,
                                closingAt,
                                responsibleForClosing))));
  }

  private AuctionEntity(
      ActorContext<Command> context,
      ReplicationContext replicationContext,
      TimerScheduler<Command> timers,
      Bid initialBid,
      Instant closingAt,
      boolean responsibleForClosing) {
    super(replicationContext);
    this.context = context;
    this.timers = timers;
    this.initialBid = initialBid;
    this.closingAt = closingAt;
    this.responsibleForClosing = responsibleForClosing;
  }

  @Override
  public AuctionState emptyState() {
    return new AuctionState(true, initialBid, initialBid.offer, Collections.emptySet());
  }

  @Override
  public SignalHandler<AuctionState> signalHandler() {
    return newSignalHandlerBuilder()
        .onSignal(RecoveryCompleted.instance(), this::onRecoveryCompleted)
        .build();
  }

  private void onRecoveryCompleted(AuctionState state) {
    if (shouldClose(state)) {
      context.getSelf().tell(Close.INSTANCE);
    }

    long millisUntilClosing =
        closingAt.toEpochMilli() - getReplicationContext().currentTimeMillis();
    timers.startSingleTimer(Finish.INSTANCE, Duration.ofMillis(millisUntilClosing));
  }

  // #setup

  // #command-handler
  @Override
  public CommandHandler<Command, Event, AuctionState> commandHandler() {

    CommandHandlerBuilder<Command, Event, AuctionState> builder = newCommandHandlerBuilder();

    // running
    builder
        .forState(state -> state.stillRunning)
        .onCommand(
            OfferBid.class,
            (state, bid) ->
                Effect()
                    .persist(
                        new BidRegistered(
                            new Bid(
                                bid.bidder,
                                bid.offer,
                                Instant.ofEpochMilli(
                                    this.getReplicationContext().currentTimeMillis()),
                                this.getReplicationContext().replicaId()))))
        .onCommand(
            GetHighestBid.class,
            (state, get) -> {
              get.replyTo.tell(state.highestBid);
              return Effect().none();
            })
        .onCommand(
            Finish.class,
            (state, finish) ->
                Effect().persist(new AuctionFinished(getReplicationContext().replicaId())))
        .onCommand(Close.class, (state, close) -> Effect().unhandled())
        .onCommand(
            IsClosed.class,
            (state, get) -> {
              get.replyTo.tell(false);
              return Effect().none();
            });

    // finished
    builder
        .forAnyState()
        .onCommand(OfferBid.class, (state, bid) -> Effect().unhandled())
        .onCommand(
            GetHighestBid.class,
            (state, get) -> {
              get.replyTo.tell(state.highestBid);
              return Effect().none();
            })
        .onCommand(
            Finish.class,
            (state, finish) ->
                Effect().persist(new AuctionFinished(getReplicationContext().replicaId())))
        .onCommand(
            Close.class,
            (state, close) ->
                Effect()
                    .persist(
                        new WinnerDecided(
                            getReplicationContext().replicaId(),
                            state.highestBid,
                            state.highestCounterOffer)))
        .onCommand(
            IsClosed.class,
            (state, get) -> {
              get.replyTo.tell(state.isClosed());
              return Effect().none();
            });

    return builder.build();
  }
  // #command-handler

  // #event-handler
  @Override
  public EventHandler<AuctionState, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(
            BidRegistered.class,
            (state, event) -> {
              if (AuctionState.isHigherBid(event.bid, state.highestBid)) {
                return state.withNewHighestBid(event.bid);
              } else {
                return state.withTooLowBid(event.bid);
              }
            })
        .onEvent(
            AuctionFinished.class,
            (state, event) -> {
              AuctionState newState = state.addFinishedAtReplica(event.atReplica.id());
              if (state.isClosed()) return state; // already closed
              else if (!getReplicationContext().recoveryRunning()) {
                eventTriggers(event, newState);
              }
              return newState;
            })
        .onEvent(WinnerDecided.class, (state, event) -> state.close())
        .build();
  }
  // #event-handler

  // #event-triggers
  private void eventTriggers(AuctionFinished event, AuctionState newState) {
    if (newState.finishedAtDc.contains(getReplicationContext().replicaId().id())) {
      if (shouldClose(newState)) {
        context.getSelf().tell(Close.INSTANCE);
      }
    } else {
      context.getSelf().tell(Finish.INSTANCE);
    }
  }

  private boolean shouldClose(AuctionState state) {
    return responsibleForClosing
        && !state.isClosed()
        && getReplicationContext().getAllReplicas().stream()
            .map(ReplicaId::id)
            .collect(Collectors.toSet())
            .equals(state.finishedAtDc);
  }
  // #event-triggers

  // #setup
}
// #setup
