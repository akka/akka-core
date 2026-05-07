/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2a

import akka.serialization.jackson.JsonSerializable

// #rename-class
case class OrderPlaced(shoppingCartId: String) extends JsonSerializable
// #rename-class
