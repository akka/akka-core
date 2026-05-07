/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed.auction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** The auction state. */
public final class AuctionState {

  /** The auction details. */
  private final Optional<Auction> auction;
  /** The status of the auction. */
  private final AuctionStatus status;
  /** The bidding history for the auction. */
  private final List<Bid> biddingHistory;

  public AuctionState(Optional<Auction> auction, AuctionStatus status, List<Bid> biddingHistory) {
    this.auction = auction;
    this.status = status;
    this.biddingHistory = biddingHistory;
  }

  public static AuctionState notStarted() {
    return new AuctionState(Optional.empty(), AuctionStatus.NOT_STARTED, Collections.emptyList());
  }

  public static AuctionState start(Auction auction) {
    return new AuctionState(
        Optional.of(auction), AuctionStatus.UNDER_AUCTION, Collections.emptyList());
  }

  public AuctionState withStatus(AuctionStatus status) {
    return new AuctionState(auction, status, biddingHistory);
  }

  public AuctionState bid(Bid bid) {
    if (lastBid().filter(b -> b.getBidder().equals(bid.getBidder())).isPresent()) {
      // Current bidder has updated their bid
      List<Bid> newBiddingHistory = new ArrayList<>(biddingHistory);
      newBiddingHistory.remove(newBiddingHistory.size() - 1); // remove last
      newBiddingHistory.add(bid);
      return new AuctionState(auction, status, newBiddingHistory);
    } else {
      List<Bid> newBiddingHistory = new ArrayList<>(biddingHistory);
      newBiddingHistory.add(bid);
      return new AuctionState(auction, status, newBiddingHistory);
    }
  }

  public Optional<Bid> lastBid() {
    if (biddingHistory.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(biddingHistory.get(biddingHistory.size() - 1));
    }
  }

  public Optional<Auction> getAuction() {
    return auction;
  }

  public AuctionStatus getStatus() {
    return status;
  }
}
