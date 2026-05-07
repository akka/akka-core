/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2a;

// #structural
import akka.serialization.jackson.JacksonMigration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CustomerMigration extends JacksonMigration {

  @Override
  public int currentVersion() {
    return 2;
  }

  @Override
  public JsonNode transform(int fromVersion, JsonNode json) {
    ObjectNode root = (ObjectNode) json;
    if (fromVersion <= 1) {
      ObjectNode shippingAddress = root.withObject("/shippingAddress");
      shippingAddress.set("street", root.get("street"));
      shippingAddress.set("city", root.get("city"));
      shippingAddress.set("zipCode", root.get("zipCode"));
      shippingAddress.set("country", root.get("country"));
      root.remove("street");
      root.remove("city");
      root.remove("zipCode");
      root.remove("country");
    }
    return root;
  }
}
// #structural
