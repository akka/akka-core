/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JavaTestEventMigrationV3 extends JacksonMigration {

  @Override
  public int currentVersion() {
    return 3;
  }

  @Override
  public String transformClassName(int fromVersion, String className) {
    // Always produce the type of the currentVersion. When fromVersion is lower,
    // transform will lift it. when fromVersion is higher, transform will adapt it.
    return JavaTestMessages.Event3.class.getName();
  }

  @Override
  public JsonNode transform(int fromVersion, JsonNode json) {
    ObjectNode root = (ObjectNode) json;
    if (fromVersion < 2) {
      root = upcastV1ToV2(root);
    }
    if (fromVersion < 3) {
      root = upcastV2ToV3(root);
    }
    return root;
  }

  private ObjectNode upcastV1ToV2(ObjectNode root) {
    root.set("field1V2", root.get("field1"));
    root.remove("field1");
    root.set("field2", IntNode.valueOf(17));
    return root;
  }

  private ObjectNode upcastV2ToV3(ObjectNode root) {
    root.set("field3", root.get("field2"));
    root.remove("field2");
    return root;
  }
}
