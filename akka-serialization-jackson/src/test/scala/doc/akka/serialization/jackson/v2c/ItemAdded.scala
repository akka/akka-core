/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2c

import akka.serialization.jackson.JsonSerializable

// #rename
case class ItemAdded(shoppingCartId: String, itemId: String, quantity: Int) extends JsonSerializable
// #rename
