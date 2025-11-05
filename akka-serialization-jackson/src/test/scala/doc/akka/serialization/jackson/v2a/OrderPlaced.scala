/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package doc.akka.serialization.jackson.v2a

import doc.akka.serialization.jackson.MySerializable

// #rename-class
case class OrderPlaced(shoppingCartId: String) extends MySerializable
// #rename-class
