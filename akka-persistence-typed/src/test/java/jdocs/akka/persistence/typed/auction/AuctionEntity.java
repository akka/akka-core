/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed.auction;

import static jdocs.akka.persistence.typed.auction.AuctionCommand.*;
import static jdocs.akka.persistence.typed.auction.AuctionEvent.*;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

/**
 * Based on
 * https://github.com/lagom/online-auction-java/blob/master/bidding-impl/src/main/java/com/example/auction/bidding/impl/AuctionEntity.java
 */
public class AuctionEntity
    extends EventSourcedBehaviorWithEnforcedReplies<AuctionCommand, AuctionEvent, AuctionState> {

  private final UUID entityUUID;

  public AuctionEntity(String entityId) {
    super(PersistenceId.ofUniqueId("Auction|" + entityId));
    this.entityUUID = UUID.fromString(entityId);
  }

  // Command handler for the not started state.
  private CommandHandlerWithReplyBuilderByState<
          AuctionCommand, AuctionEvent, AuctionState, AuctionState>
      notStartedHandler =
          newCommandHandlerWithReplyBuilder()
              .forState(state -> state.getStatus() == AuctionStatus.NOT_STARTED)
              .onCommand(StartAuction.class, this::startAuction)
              .onCommand(
                  PlaceBid.class,
                  (state, cmd) ->
                      Effect().reply(cmd.replyTo, createResult(state, PlaceBidStatus.NOT_STARTED)));

  // Command handler for the under auction state.
  private CommandHandlerWithReplyBuilderByState<
          AuctionCommand, AuctionEvent, AuctionState, AuctionState>
      underAuctionHandler =
          newCommandHandlerWithReplyBuilder()
              .forState(state -> state.getStatus() == AuctionStatus.UNDER_AUCTION)
              .onCommand(StartAuction.class, (state, cmd) -> alreadyDone(cmd.replyTo))
              .onCommand(PlaceBid.class, this::placeBid)
              .onCommand(FinishBidding.class, this::finishBidding);

  // Command handler for the completed state.
  private CommandHandlerWithReplyBuilderByState<
          AuctionCommand, AuctionEvent, AuctionState, AuctionState>
      completedHandler =
          newCommandHandlerWithReplyBuilder()
              .forState(state -> state.getStatus() == AuctionStatus.COMPLETE)
              .onCommand(StartAuction.class, (state, cmd) -> alreadyDone(cmd.replyTo))
              .onCommand(FinishBidding.class, (state, cmd) -> alreadyDone(cmd.replyTo))
              .onCommand(
                  PlaceBid.class,
                  (state, cmd) ->
                      Effect().reply(cmd.replyTo, createResult(state, PlaceBidStatus.FINISHED)));

  // Command handler for the cancelled state.
  private CommandHandlerWithReplyBuilderByState<
          AuctionCommand, AuctionEvent, AuctionState, AuctionState>
      cancelledHandler =
          newCommandHandlerWithReplyBuilder()
              .forState(state -> state.getStatus() == AuctionStatus.CANCELLED)
              .onCommand(StartAuction.class, (state, cmd) -> alreadyDone(cmd.replyTo))
              .onCommand(FinishBidding.class, (state, cmd) -> alreadyDone(cmd.replyTo))
              .onCommand(CancelAuction.class, (state, cmd) -> alreadyDone(cmd.replyTo))
              .onCommand(
                  PlaceBid.class,
                  (state, cmd) ->
                      Effect().reply(cmd.replyTo, createResult(state, PlaceBidStatus.CANCELLED)));

  private CommandHandlerWithReplyBuilderByState<
          AuctionCommand, AuctionEvent, AuctionState, AuctionState>
      getAuctionHandler =
          newCommandHandlerWithReplyBuilder()
              .forStateType(AuctionState.class)
              .onCommand(GetAuction.class, (state, cmd) -> Effect().reply(cmd.replyTo, state));

  private CommandHandlerBuilderByState<AuctionCommand, AuctionEvent, AuctionState, AuctionState>
      cancelHandler =
          newCommandHandlerBuilder()
              .forStateType(AuctionState.class)
              .onCommand(CancelAuction.class, this::cancelAuction);
  // Note, an item can go from completed to cancelled, since it is the item service that controls
  // whether an auction is cancelled or not. If it cancels before it receives a bidding finished
  // event from us, it will ignore the bidding finished event, so we need to update our state to
  // reflect that.

  private ReplyEffect<AuctionEvent, AuctionState> startAuction(
      AuctionState state, StartAuction cmd) {
    return Effect()
        .persist(new AuctionStarted(entityUUID, cmd.auction))
        .thenReply(cmd.replyTo, notUsed -> Done.getInstance());
  }

  private ReplyEffect<AuctionEvent, AuctionState> finishBidding(
      AuctionState state, FinishBidding cmd) {
    return Effect()
        .persist(new BiddingFinished(entityUUID))
        .thenReply(cmd.replyTo, notUsed -> Done.getInstance());
  }

  private ReplyEffect<AuctionEvent, AuctionState> cancelAuction(
      AuctionState state, CancelAuction cmd) {
    return Effect()
        .persist(new AuctionCancelled(entityUUID))
        .thenReply(cmd.replyTo, notUsed -> Done.getInstance());
  }

  /** The main logic for handling of bids. */
  private ReplyEffect<AuctionEvent, AuctionState> placeBid(AuctionState state, PlaceBid bid) {
    Auction auction = state.getAuction().get();

    Instant now = Instant.now();

    // Even though we're not in the finished state yet, we should check
    if (auction.getEndTime().isBefore(now)) {
      return Effect().reply(bid.replyTo, createResult(state, PlaceBidStatus.FINISHED));
    }

    if (auction.getCreator().equals(bid.bidder)) {
      return Effect()
          .reply(
              bid.replyTo,
              new PlaceBidRejected("An auctions creator cannot bid in their own auction."));
    }

    Optional<Bid> currentBid = state.lastBid();
    int currentBidPrice;
    int currentBidMaximum;
    if (currentBid.isPresent()) {
      currentBidPrice = currentBid.get().getBidPrice();
      currentBidMaximum = currentBid.get().getMaximumBid();
    } else {
      currentBidPrice = 0;
      currentBidMaximum = 0;
    }

    boolean bidderIsCurrentBidder =
        currentBid.filter(b -> b.getBidder().equals(bid.bidder)).isPresent();

    if (bidderIsCurrentBidder && bid.bidPrice >= currentBidPrice) {
      // Allow the current bidder to update their bid
      if (auction.getReservePrice() > currentBidPrice) {

        int newBidPrice = Math.min(auction.getReservePrice(), bid.bidPrice);
        PlaceBidStatus placeBidStatus;

        if (newBidPrice == auction.getReservePrice()) {
          placeBidStatus = PlaceBidStatus.ACCEPTED;
        } else {
          placeBidStatus = PlaceBidStatus.ACCEPTED_BELOW_RESERVE;
        }
        return Effect()
            .persist(new BidPlaced(entityUUID, new Bid(bid.bidder, now, newBidPrice, bid.bidPrice)))
            .thenReply(
                bid.replyTo,
                newState -> new PlaceBidResult(placeBidStatus, newBidPrice, bid.bidder));
      }
      return Effect()
          .persist(
              new BidPlaced(entityUUID, new Bid(bid.bidder, now, currentBidPrice, bid.bidPrice)))
          .thenReply(
              bid.replyTo,
              newState -> new PlaceBidResult(PlaceBidStatus.ACCEPTED, currentBidPrice, bid.bidder));
    }

    if (bid.bidPrice < currentBidPrice + auction.getIncrement()) {
      return Effect().reply(bid.replyTo, createResult(state, PlaceBidStatus.TOO_LOW));
    } else if (bid.bidPrice <= currentBidMaximum) {
      return handleAutomaticOutbid(
          bid, auction, now, currentBid, currentBidPrice, currentBidMaximum);
    } else {
      return handleNewWinningBidder(bid, auction, now, currentBidMaximum);
    }
  }

  /**
   * Handle the situation where a bid will be accepted, but it will be automatically outbid by the
   * current bidder.
   *
   * <p>This emits two events, one for the bid currently being replace, and another automatic bid
   * for the current bidder.
   */
  private ReplyEffect<AuctionEvent, AuctionState> handleAutomaticOutbid(
      PlaceBid bid,
      Auction auction,
      Instant now,
      Optional<Bid> currentBid,
      int currentBidPrice,
      int currentBidMaximum) {
    // Adjust the bid so that the increment for the current maximum makes the
    // current maximum a valid bid
    int adjustedBidPrice = Math.min(bid.bidPrice, currentBidMaximum - auction.getIncrement());
    int newBidPrice = adjustedBidPrice + auction.getIncrement();

    return Effect()
        .persist(
            Arrays.asList(
                new BidPlaced(entityUUID, new Bid(bid.bidder, now, adjustedBidPrice, bid.bidPrice)),
                new BidPlaced(
                    entityUUID,
                    new Bid(currentBid.get().getBidder(), now, newBidPrice, currentBidMaximum))))
        .thenReply(
            bid.replyTo,
            newState ->
                new PlaceBidResult(
                    PlaceBidStatus.ACCEPTED_OUTBID, newBidPrice, currentBid.get().getBidder()));
  }

  /** Handle the situation where a bid will be accepted as the new winning bidder. */
  private ReplyEffect<AuctionEvent, AuctionState> handleNewWinningBidder(
      PlaceBid bid, Auction auction, Instant now, int currentBidMaximum) {
    int nextIncrement = Math.min(currentBidMaximum + auction.getIncrement(), bid.bidPrice);
    int newBidPrice;
    if (nextIncrement < auction.getReservePrice()) {
      newBidPrice = Math.min(auction.getReservePrice(), bid.bidPrice);
    } else {
      newBidPrice = nextIncrement;
    }
    return Effect()
        .persist(new BidPlaced(entityUUID, new Bid(bid.bidder, now, newBidPrice, bid.bidPrice)))
        .thenReply(
            bid.replyTo,
            newState -> {
              PlaceBidStatus status;
              if (newBidPrice < auction.getReservePrice()) {
                status = PlaceBidStatus.ACCEPTED_BELOW_RESERVE;
              } else {
                status = PlaceBidStatus.ACCEPTED;
              }
              return new PlaceBidResult(status, newBidPrice, bid.bidder);
            });
  }

  @Override
  public AuctionState emptyState() {
    return AuctionState.notStarted();
  }

  @Override
  public CommandHandlerWithReply<AuctionCommand, AuctionEvent, AuctionState> commandHandler() {
    return notStartedHandler
        .orElse(underAuctionHandler)
        .orElse(completedHandler)
        .orElse(getAuctionHandler)
        .orElse(cancelledHandler)
        .build();
  }

  @Override
  public EventHandler<AuctionState, AuctionEvent> eventHandler() {

    EventHandlerBuilder<AuctionState, AuctionEvent> builder = newEventHandlerBuilder();

    builder
        .forState(auction -> auction.getStatus() == AuctionStatus.NOT_STARTED)
        .onEvent(AuctionStarted.class, (state, evt) -> AuctionState.start(evt.getAuction()));

    builder
        .forState(auction -> auction.getStatus() == AuctionStatus.UNDER_AUCTION)
        .onEvent(BidPlaced.class, (state, evt) -> state.bid(evt.getBid()))
        .onEvent(BiddingFinished.class, (state, evt) -> state.withStatus(AuctionStatus.COMPLETE))
        .onEvent(AuctionCancelled.class, (state, evt) -> state.withStatus(AuctionStatus.CANCELLED));

    return builder.build();
  }

  private PlaceBidResult createResult(AuctionState state, PlaceBidStatus status) {
    Optional<Bid> lastBid = state.lastBid();
    if (lastBid.isPresent()) {
      Bid bid = lastBid.get();
      return new PlaceBidResult(status, bid.getBidPrice(), bid.getBidder());
    } else {
      return new PlaceBidResult(status, 0, null);
    }
  }

  private ReplyEffect<AuctionEvent, AuctionState> alreadyDone(ActorRef<Done> replyTo) {
    return Effect().reply(replyTo, Done.getInstance());
  }
}
