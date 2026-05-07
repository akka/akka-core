/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v1;

import akka.serialization.jackson.JsonSerializable;

// #rename-class
public class OrderAdded implements JsonSerializable {
  public final String shoppingCartId;

  public OrderAdded(String shoppingCartId) {
    this.shoppingCartId = shoppingCartId;
  }
}
// #rename-class
