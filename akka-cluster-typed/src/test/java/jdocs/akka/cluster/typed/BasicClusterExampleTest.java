/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.typed;

// #join-seed-nodes
import akka.actor.Address;
import akka.actor.AddressFromURIString;
import akka.cluster.Member;
import akka.cluster.typed.JoinSeedNodes;

// #join-seed-nodes

// #cluster-imports
import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import akka.cluster.ClusterEvent;
import akka.cluster.typed.*;
// #cluster-imports
import akka.actor.testkit.typed.javadsl.TestProbe;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// FIXME use awaitAssert to await cluster forming like in BasicClusterExampleSpec
public class BasicClusterExampleTest { // extends JUnitSuite {

  private Config clusterConfig =
      ConfigFactory.parseString(
          "akka { \n"
              + "  actor.provider = cluster \n"
              + "  remote.artery { \n"
              + "    canonical { \n"
              + "      hostname = \"127.0.0.1\" \n"
              + "      port = 2551 \n"
              + "    } \n"
              + "  } \n"
              + "}  \n");

  private Config noPort =
      ConfigFactory.parseString(
              "      akka.remote.artery.canonical.port = 0 \n");

  // @Test
  public void clusterApiExample() {
    ActorSystem<Object> system =
        ActorSystem.create(Behaviors.empty(), "ClusterSystem", noPort.withFallback(clusterConfig));
    ActorSystem<Object> system2 =
        ActorSystem.create(Behaviors.empty(), "ClusterSystem", noPort.withFallback(clusterConfig));

    try {
      // #cluster-create
      Cluster cluster = Cluster.get(system);
      // #cluster-create
      Cluster cluster2 = Cluster.get(system2);

      // #cluster-join
      cluster.manager().tell(Join.create(cluster.selfMember().address()));
      // #cluster-join

      cluster2.manager().tell(Join.create(cluster.selfMember().address()));

      // TODO wait for/verify cluster to form

      // #prepare
      PrepareForFullClusterShutdown msg =
          PrepareForFullClusterShutdown$.MODULE$.prepareForFullClusterShutdown();
      cluster2.manager().tell(msg);
      // #prepare

      // #cluster-leave
      cluster2.manager().tell(Leave.create(cluster2.selfMember().address()));
      // #cluster-leave

      // TODO wait for/verify node 2 leaving

    } finally {
      system.terminate();
      system2.terminate();
    }
  }

  // @Test
  public void clusterLeave() throws Exception {
    ActorSystem<Object> system =
        ActorSystem.create(Behaviors.empty(), "ClusterSystem", noPort.withFallback(clusterConfig));
    ActorSystem<Object> system2 =
        ActorSystem.create(Behaviors.empty(), "ClusterSystem", noPort.withFallback(clusterConfig));

    try {
      Cluster cluster = Cluster.get(system);
      Cluster cluster2 = Cluster.get(system2);

      TestProbe<ClusterEvent.MemberEvent> testProbe = TestProbe.create(system);
      ActorRef<ClusterEvent.MemberEvent> subscriber = testProbe.getRef();
      // #cluster-subscribe
      cluster.subscriptions().tell(Subscribe.create(subscriber, ClusterEvent.MemberEvent.class));
      // #cluster-subscribe

      Address anotherMemberAddress = cluster2.selfMember().address();
      // #cluster-leave-example
      cluster.manager().tell(Leave.create(anotherMemberAddress));
      // subscriber will receive events MemberLeft, MemberExited and MemberRemoved
      // #cluster-leave-example
      testProbe.expectMessageClass(ClusterEvent.MemberLeft.class);
      testProbe.expectMessageClass(ClusterEvent.MemberExited.class);
      testProbe.expectMessageClass(ClusterEvent.MemberRemoved.class);

    } finally {
      system.terminate();
      system2.terminate();
    }
  }

  void illustrateJoinSeedNodes() {
    ActorSystem<Void> system = null;

    // #join-seed-nodes
    List<Address> seedNodes = new ArrayList<>();
    seedNodes.add(AddressFromURIString.parse("akka://ClusterSystem@127.0.0.1:2551"));
    seedNodes.add(AddressFromURIString.parse("akka://ClusterSystem@127.0.0.1:2552"));

    Cluster.get(system).manager().tell(new JoinSeedNodes(seedNodes));
    // #join-seed-nodes
  }

  static class Backend {
    static Behavior<Void> create() {
      return Behaviors.empty();
    }
  }

  static class Frontend {
    static Behavior<Void> create() {
      return Behaviors.empty();
    }
  }

  void illustrateRoles() {
    ActorContext<Void> context = null;

    // #hasRole
    Member selfMember = Cluster.get(context.getSystem()).selfMember();
    if (selfMember.hasRole("backend")) {
      context.spawn(Backend.create(), "back");
    } else if (selfMember.hasRole("front")) {
      context.spawn(Frontend.create(), "front");
    }
    // #hasRole
  }

  void illustrateDcAccess() {
    ActorSystem<Void> system = null;

    // #dcAccess
    final Cluster cluster = Cluster.get(system);
    // this node's data center
    String dc = cluster.selfMember().dataCenter();
    // all known data centers
    Set<String> allDc = cluster.state().getAllDataCenters();
    // a specific member's data center
    Member aMember = cluster.state().getMembers().iterator().next();
    String aDc = aMember.dataCenter();
    // #dcAccess
  }
}
