/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2a;

import com.fasterxml.jackson.annotation.JsonCreator;
import jdoc.akka.serialization.jackson.MySerializable;

// #rename-class
public class OrderPlaced implements MySerializable {
  public final String shoppingCartId;

  @JsonCreator
  public OrderPlaced(String shoppingCartId) {
    this.shoppingCartId = shoppingCartId;
  }
}
// #rename-class
