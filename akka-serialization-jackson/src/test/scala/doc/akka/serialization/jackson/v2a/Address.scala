/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2a

import akka.serialization.jackson.JsonSerializable

// #structural
case class Address(street: String, city: String, zipCode: String, country: String) extends JsonSerializable
// #structural
