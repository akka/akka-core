/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2b;

import akka.serialization.jackson.JsonSerializable;

// #add-mandatory
public class ItemAdded implements JsonSerializable {
  public final String shoppingCartId;
  public final String productId;
  public final int quantity;
  public final double discount;

  public ItemAdded(String shoppingCartId, String productId, int quantity, double discount) {
    this.shoppingCartId = shoppingCartId;
    this.productId = productId;
    this.quantity = quantity;
    this.discount = discount;
  }
}
// #add-mandatory
