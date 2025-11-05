/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package jdoc.akka.serialization.jackson.v2b;

import jdoc.akka.serialization.jackson.MySerializable;

// #add-mandatory
public class ItemAdded implements MySerializable {
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
