/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package doc.akka.serialization.jackson.v1

import doc.akka.serialization.jackson.MySerializable

// #add-optional
// #forward-one-rename
case class ItemAdded(shoppingCartId: String, productId: String, quantity: Int) extends MySerializable
// #forward-one-rename
// #add-optional
