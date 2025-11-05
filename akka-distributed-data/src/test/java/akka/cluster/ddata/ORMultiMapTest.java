/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata;

public class ORMultiMapTest {

  public void compileOnlyORMultiMapTest() {
    // primarily to check API accessibility with overloads/types
    SelfUniqueAddress node = null;
    ORMultiMap<String, String> orMultiMap = ORMultiMap.create();
    orMultiMap.addBinding(node, "a", "1");
    orMultiMap.removeBinding(node, "a", "1");
  }
}
