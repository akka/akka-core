/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2b

import akka.serialization.jackson.JsonSerializable

// #add-mandatory
case class ItemAdded(shoppingCartId: String, productId: String, quantity: Int, discount: Double)
    extends JsonSerializable
// #add-mandatory
