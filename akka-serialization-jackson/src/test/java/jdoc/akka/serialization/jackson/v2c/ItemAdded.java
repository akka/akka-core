/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package jdoc.akka.serialization.jackson.v2c;

import jdoc.akka.serialization.jackson.MySerializable;

// #rename
public class ItemAdded implements MySerializable {
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
