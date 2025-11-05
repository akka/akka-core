/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster;

import akka.actor.ActorSystem;
import akka.actor.Address;
import java.util.Collections;
import java.util.List;

// Doc code, compile only
@SuppressWarnings("ConstantConditions")
public class ClusterJavaCompileTest {

  final ActorSystem system = null;
  final Cluster cluster = null;

  public void compileJoinSeedNodesInJava() {
    final List<Address> addresses = Collections.singletonList(new Address("akka", "MySystem"));
    cluster.joinSeedNodes(addresses);
  }
}
