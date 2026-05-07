/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v1withv2

// #forward-one-rename
import akka.serialization.jackson.JacksonMigration
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

class ItemAddedMigration extends JacksonMigration {

  // Data produced in this node is still produced using the version 1 of the schema
  override def currentVersion: Int = 1

  override def supportedForwardVersion: Int = 2

  override def transform(fromVersion: Int, json: JsonNode): JsonNode = {
    val root = json.asInstanceOf[ObjectNode]
    if (fromVersion == 2) {
      // When receiving an event of version 2 we down-cast it to the version 1 of the schema
      root.set[JsonNode]("productId", root.get("itemId"))
      root.remove("itemId")
    }
    root
  }
}
// #forward-one-rename
