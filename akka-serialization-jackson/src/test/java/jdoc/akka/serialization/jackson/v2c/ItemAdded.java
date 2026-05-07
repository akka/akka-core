/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2c;

import akka.serialization.jackson.JsonSerializable;

// #rename
public class ItemAdded implements JsonSerializable {
  public final String shoppingCartId;

  public final String itemId;

  public final int quantity;

  public ItemAdded(String shoppingCartId, String itemId, int quantity) {
    this.shoppingCartId = shoppingCartId;
    this.itemId = itemId;
    this.quantity = quantity;
  }
}
// #rename
