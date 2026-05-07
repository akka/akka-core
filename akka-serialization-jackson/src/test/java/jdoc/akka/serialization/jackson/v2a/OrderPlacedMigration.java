/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2a;

import akka.serialization.jackson.JacksonMigration;
import com.fasterxml.jackson.databind.JsonNode;

// #rename-class
public class OrderPlacedMigration extends JacksonMigration {

  @Override
  public int currentVersion() {
    return 2;
  }

  @Override
  public String transformClassName(int fromVersion, String className) {
    return OrderPlaced.class.getName();
  }

  @Override
  public JsonNode transform(int fromVersion, JsonNode json) {
    return json;
  }
}
// #rename-class
