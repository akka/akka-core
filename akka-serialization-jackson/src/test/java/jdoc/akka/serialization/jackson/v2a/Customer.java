/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2a;

import java.util.Optional;
import jdoc.akka.serialization.jackson.MySerializable;

// #structural
public class Customer implements MySerializable {
  public final String name;
  public final Address shippingAddress;
  public final Optional<Address> billingAddress;

  public Customer(String name, Address shippingAddress, Optional<Address> billingAddress) {
    this.name = name;
    this.shippingAddress = shippingAddress;
    this.billingAddress = billingAddress;
  }
}
// #structural
