/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v1;

import jdoc.akka.serialization.jackson.MySerializable;

// #add-optional
// #forward-one-rename
public class ItemAdded implements MySerializable {
  public final String shoppingCartId;
  public final String productId;
  public final int quantity;

  public ItemAdded(String shoppingCartId, String productId, int quantity) {
    this.shoppingCartId = shoppingCartId;
    this.productId = productId;
    this.quantity = quantity;
  }
}
// #forward-one-rename
// #add-optional
