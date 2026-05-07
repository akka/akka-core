/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2a

import akka.serialization.jackson.JsonSerializable
import com.fasterxml.jackson.annotation.JsonCreator

// #add-optional
case class ItemAdded(shoppingCartId: String, productId: String, quantity: Int, discount: Option[Double], note: String)
    extends JsonSerializable {

  // alternative constructor because `note` should have default value "" when not defined in json
  @JsonCreator
  def this(shoppingCartId: String, productId: String, quantity: Int, discount: Option[Double], note: Option[String]) =
    this(shoppingCartId, productId, quantity, discount, note.getOrElse(""))
}
// #add-optional
