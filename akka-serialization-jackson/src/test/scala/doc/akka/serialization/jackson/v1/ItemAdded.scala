/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v1

import akka.serialization.jackson.JsonSerializable

// #add-optional
// #forward-one-rename
case class ItemAdded(shoppingCartId: String, productId: String, quantity: Int) extends JsonSerializable
// #forward-one-rename
// #add-optional
