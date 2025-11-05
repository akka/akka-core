/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2c

// #rename
import akka.serialization.jackson.JacksonMigration
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

class ItemAddedMigration extends JacksonMigration {

  override def currentVersion: Int = 2

  override def transform(fromVersion: Int, json: JsonNode): JsonNode = {
    val root = json.asInstanceOf[ObjectNode]
    if (fromVersion <= 1) {
      root.set[JsonNode]("itemId", root.get("productId"))
      root.remove("productId")
    }
    root
  }
}
// #rename
