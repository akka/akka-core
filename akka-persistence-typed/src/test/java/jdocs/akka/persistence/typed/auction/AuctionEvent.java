/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed.auction;

import java.util.UUID;

/** A persisted auction event. */
public interface AuctionEvent {

  /** The auction started. */
  final class AuctionStarted implements AuctionEvent {

    /** The item that the auction started on. */
    private final UUID itemId;
    /** The auction details. */
    private final Auction auction;

    public AuctionStarted(UUID itemId, Auction auction) {
      this.itemId = itemId;
      this.auction = auction;
    }

    public UUID getItemId() {
      return itemId;
    }

    public Auction getAuction() {
      return auction;
    }
  }

  /** A bid was placed. */
  final class BidPlaced implements AuctionEvent {

    /** The item that the bid was placed on. */
    private final UUID itemId;
    /** The bid. */
    private final Bid bid;

    public BidPlaced(UUID itemId, Bid bid) {
      this.itemId = itemId;
      this.bid = bid;
    }

    public UUID getItemId() {
      return itemId;
    }

    public Bid getBid() {
      return bid;
    }
  }

  /** Bidding finished. */
  final class BiddingFinished implements AuctionEvent {

    /** The item that bidding finished for. */
    private final UUID itemId;

    public BiddingFinished(UUID itemId) {
      this.itemId = itemId;
    }

    public UUID getItemId() {
      return itemId;
    }
  }

  /** The auction was cancelled. */
  final class AuctionCancelled implements AuctionEvent {

    /** The item that the auction was cancelled for. */
    private final UUID itemId;

    public AuctionCancelled(UUID itemId) {
      this.itemId = itemId;
    }

    public UUID getItemId() {
      return itemId;
    }
  }
}
