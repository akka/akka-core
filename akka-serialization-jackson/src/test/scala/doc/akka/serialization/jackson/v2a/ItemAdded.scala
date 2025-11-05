/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package doc.akka.serialization.jackson.v2a

import com.fasterxml.jackson.annotation.JsonCreator
import jdoc.akka.serialization.jackson.MySerializable

// #add-optional
case class ItemAdded(shoppingCartId: String, productId: String, quantity: Int, discount: Option[Double], note: String)
    extends MySerializable {

  // alternative constructor because `note` should have default value "" when not defined in json
  @JsonCreator
  def this(shoppingCartId: String, productId: String, quantity: Int, discount: Option[Double], note: Option[String]) =
    this(shoppingCartId, productId, quantity, discount, note.getOrElse(""))
}
// #add-optional
