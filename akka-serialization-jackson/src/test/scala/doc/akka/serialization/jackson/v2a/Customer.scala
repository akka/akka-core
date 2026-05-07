/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2a

import akka.serialization.jackson.JsonSerializable

// #structural
case class Customer(name: String, shippingAddress: Address, billingAddress: Option[Address]) extends JsonSerializable
// #structural
