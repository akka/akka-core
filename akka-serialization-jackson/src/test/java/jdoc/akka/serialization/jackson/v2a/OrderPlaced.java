/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2a;

import akka.serialization.jackson.JsonSerializable;

// #rename-class
public class OrderPlaced implements JsonSerializable {
  public final String shoppingCartId;

  public OrderPlaced(String shoppingCartId) {
    this.shoppingCartId = shoppingCartId;
  }
}
// #rename-class
