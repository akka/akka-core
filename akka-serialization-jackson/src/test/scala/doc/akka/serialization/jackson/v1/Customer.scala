/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v1

import doc.akka.serialization.jackson.MySerializable

// #structural
case class Customer(name: String, street: String, city: String, zipCode: String, country: String) extends MySerializable
// #structural
