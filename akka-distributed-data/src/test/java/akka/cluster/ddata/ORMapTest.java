/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata;

public class ORMapTest {

  public void compileOnlyORMapTest() {
    // primarily to check API accessibility with overloads/types
    SelfUniqueAddress node1 = null;

    ORMap<String, PNCounterMap<String>> orMap = ORMap.create();
    // updated needs a cast
    ORMap<String, PNCounterMap<String>> updated =
        orMap.update(node1, "key", PNCounterMap.create(), curr -> curr.decrement(node1, "key", 10));
    updated.getEntries();
  }
}
