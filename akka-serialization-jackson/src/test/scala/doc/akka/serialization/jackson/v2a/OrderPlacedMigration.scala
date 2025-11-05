/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson.v2a

import akka.serialization.jackson.JacksonMigration
import com.fasterxml.jackson.databind.JsonNode

// #rename-class
class OrderPlacedMigration extends JacksonMigration {

  override def currentVersion: Int = 2

  override def transformClassName(fromVersion: Int, className: String): String =
    classOf[OrderPlaced].getName

  override def transform(fromVersion: Int, json: JsonNode): JsonNode = json
}
// #rename-class
