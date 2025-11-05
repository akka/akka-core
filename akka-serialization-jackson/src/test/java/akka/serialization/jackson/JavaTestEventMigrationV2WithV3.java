/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JavaTestEventMigrationV2WithV3 extends JacksonMigration {

  @Override
  public int currentVersion() {
    return 2;
  }

  @Override
  public int supportedForwardVersion() {
    return 3;
  }

  @Override
  public String transformClassName(int fromVersion, String className) {
    // Always produce the type of the currentVersion. When fromVersion is lower,
    // transform will lift it. When fromVersion is higher, transform will downcast it.
    return JavaTestMessages.Event2.class.getName();
  }

  @Override
  public JsonNode transform(int fromVersion, JsonNode json) {
    ObjectNode root = (ObjectNode) json;
    if (fromVersion < 2) {
      root = upcastV1ToV2((ObjectNode) json);
    }
    if (fromVersion == 3) {
      root = downcastV3ToV2((ObjectNode) json);
    }
    return root;
  }

  private ObjectNode upcastV1ToV2(ObjectNode json) {
    ObjectNode root = json;
    root.set("field1V2", root.get("field1"));
    root.remove("field1");
    root.set("field2", IntNode.valueOf(17));
    return root;
  }

  private ObjectNode downcastV3ToV2(ObjectNode json) {
    ObjectNode root = json;
    root.set("field2", root.get("field3"));
    root.remove("field3");
    return root;
  }
}
