/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed.auction;

/** Auction status. */
public enum AuctionStatus {
  /** The auction hasn't started yet (or doesn't exist). */
  NOT_STARTED,
  /** The item is under auction. */
  UNDER_AUCTION,
  /** The auction is complete. */
  COMPLETE,
  /** The auction is cancelled. */
  CANCELLED
}
