/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package doc.akka.serialization.jackson.v1

import doc.akka.serialization.jackson.MySerializable

// #rename-class
case class OrderAdded(shoppingCartId: String) extends MySerializable
// #rename-class
