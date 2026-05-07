/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v1

import akka.serialization.jackson.JsonSerializable

// #rename-class
case class OrderAdded(shoppingCartId: String) extends JsonSerializable
// #rename-class
