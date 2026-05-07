/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import akka.testkit.javadsl.TestKit;
import com.typesafe.config.ConfigFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// #join-seed-nodes-imports
import akka.actor.Address;
import akka.cluster.Cluster;

// #join-seed-nodes-imports
import akka.actor.ActorSystem;
import akka.cluster.Member;

public class ClusterDocTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system =
        ActorSystem.create(
            "ClusterDocTest",
            ConfigFactory.parseString(scala.docs.cluster.ClusterDocSpec.config()));
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void demonstrateLeave() {
    // #leave
    final Cluster cluster = Cluster.get(system);
    cluster.leave(cluster.selfAddress());
    // #leave

  }

  // compile only
  @SuppressWarnings("unused")
  public void demonstrateDataCenter() {
    // #dcAccess
    final Cluster cluster = Cluster.get(system);
    // this node's data center
    String dc = cluster.selfDataCenter();
    // all known data centers
    Set<String> allDc = cluster.state().getAllDataCenters();
    // a specific member's data center
    Member aMember = cluster.state().getMembers().iterator().next();
    String aDc = aMember.dataCenter();
    // #dcAccess
  }

  // compile only
  @SuppressWarnings("unused")
  public void demonstrateJoinSeedNodes() {
    // #join-seed-nodes
    final Cluster cluster = Cluster.get(system);
    List<Address> list =
        new LinkedList<>(); // replace this with your method to dynamically get seed nodes
    cluster.joinSeedNodes(list);
    // #join-seed-nodes
  }
}
