/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package doc.akka.serialization.jackson.v2c

import doc.akka.serialization.jackson.MySerializable

// #rename
case class ItemAdded(shoppingCartId: String, itemId: String, quantity: Int) extends MySerializable
// #rename
