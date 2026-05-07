/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed.auction;

import akka.Done;
import akka.actor.typed.ActorRef;
import java.util.UUID;

/** An auction command. */
public interface AuctionCommand {

  /** Start the auction. */
  final class StartAuction implements AuctionCommand {

    /** The auction to start. */
    public final Auction auction;

    public final ActorRef<Done> replyTo;

    public StartAuction(Auction auction, ActorRef<Done> replyTo) {
      this.auction = auction;
      this.replyTo = replyTo;
    }
  }

  /** Cancel the auction. */
  final class CancelAuction implements AuctionCommand {
    public final ActorRef<Done> replyTo;

    public CancelAuction(ActorRef<Done> replyTo) {
      this.replyTo = replyTo;
    }
  }

  /** Place a bid on the auction. */
  final class PlaceBid implements AuctionCommand {

    public final int bidPrice;
    public final UUID bidder;
    public final ActorRef<PlaceBidReply> replyTo;

    public PlaceBid(int bidPrice, UUID bidder, ActorRef<PlaceBidReply> replyTo) {
      this.bidPrice = bidPrice;
      this.bidder = bidder;
      this.replyTo = replyTo;
    }
  }

  interface PlaceBidReply {}

  /** The status of placing a bid. */
  enum PlaceBidStatus {
    /** The bid was accepted, and is the current highest bid. */
    ACCEPTED(BidResultStatus.ACCEPTED),
    /** The bid was accepted, but was outbidded by the maximum bid of the current highest bidder. */
    ACCEPTED_OUTBID(BidResultStatus.ACCEPTED_OUTBID),
    /** The bid was accepted, but is below the reserve. */
    ACCEPTED_BELOW_RESERVE(BidResultStatus.ACCEPTED_BELOW_RESERVE),
    /** The bid was not at least the current bid plus the increment. */
    TOO_LOW(BidResultStatus.TOO_LOW),
    /** The auction hasn't started. */
    NOT_STARTED(BidResultStatus.NOT_STARTED),
    /** The auction has already finished. */
    FINISHED(BidResultStatus.FINISHED),
    /** The auction has been cancelled. */
    CANCELLED(BidResultStatus.CANCELLED);

    public final BidResultStatus bidResultStatus;

    PlaceBidStatus(BidResultStatus bidResultStatus) {
      this.bidResultStatus = bidResultStatus;
    }

    public static PlaceBidStatus from(BidResultStatus status) {
      switch (status) {
        case ACCEPTED:
          return ACCEPTED;
        case ACCEPTED_BELOW_RESERVE:
          return ACCEPTED_BELOW_RESERVE;
        case ACCEPTED_OUTBID:
          return ACCEPTED_OUTBID;
        case CANCELLED:
          return CANCELLED;
        case FINISHED:
          return FINISHED;
        case NOT_STARTED:
          return NOT_STARTED;
        case TOO_LOW:
          return TOO_LOW;
        default:
          throw new IllegalStateException();
      }
    }
  }

  /** The result of placing a bid. */
  final class PlaceBidResult implements PlaceBidReply {

    /** The current price of the auction. */
    private final int currentPrice;
    /** The status of the attempt to place a bid. */
    private final PlaceBidStatus status;
    /** The current winning bidder. */
    private final UUID currentBidder;

    public PlaceBidResult(PlaceBidStatus status, int currentPrice, UUID currentBidder) {
      this.currentPrice = currentPrice;
      this.status = status;
      this.currentBidder = currentBidder;
    }

    public int getCurrentPrice() {
      return currentPrice;
    }

    public PlaceBidStatus getStatus() {
      return status;
    }

    public UUID getCurrentBidder() {
      return currentBidder;
    }
  }

  final class PlaceBidRejected implements PlaceBidReply {
    private final String errorMessage;

    public PlaceBidRejected(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }

  /** Finish bidding. */
  final class FinishBidding implements AuctionCommand {

    public final ActorRef<Done> replyTo;

    FinishBidding(ActorRef<Done> replyTo) {
      this.replyTo = replyTo;
    }
  }

  /** Get the auction. */
  final class GetAuction implements AuctionCommand {
    public final ActorRef<AuctionState> replyTo;

    public GetAuction(ActorRef<AuctionState> replyTo) {
      this.replyTo = replyTo;
    }
  }
}
