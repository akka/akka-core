/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v1withv2;

// #forward-one-rename

import akka.serialization.jackson.JacksonMigration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ItemAddedMigration extends JacksonMigration {

  // Data produced in this node is still produced using the version 1 of the schema
  @Override
  public int currentVersion() {
    return 1;
  }

  @Override
  public int supportedForwardVersion() {
    return 2;
  }

  @Override
  public JsonNode transform(int fromVersion, JsonNode json) {
    ObjectNode root = (ObjectNode) json;
    if (fromVersion == 2) {
      // When receiving an event of version 2 we down-cast it to the version 1 of the schema
      root.set("productId", root.get("itemId"));
      root.remove("itemId");
    }
    return root;
  }
}
// #forward-one-rename
