/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package doc.akka.serialization.jackson.v2a

import doc.akka.serialization.jackson.MySerializable

// #structural
case class Address(street: String, city: String, zipCode: String, country: String) extends MySerializable
// #structural
