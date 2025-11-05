/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Cancellable;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.ReachabilityEvent;
import akka.cluster.ClusterEvent.ReachableMember;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import jdocs.cluster.StatsMessages.JobFailed;
import jdocs.cluster.StatsMessages.StatsJob;
import jdocs.cluster.StatsMessages.StatsResult;

public class StatsSampleClient extends AbstractActor {

  final String servicePath;
  final Cancellable tickTask;
  final Set<Address> nodes = new HashSet<Address>();

  Cluster cluster = Cluster.get(getContext().getSystem());

  public StatsSampleClient(String servicePath) {
    this.servicePath = servicePath;
    Duration interval = Duration.ofMillis(2);
    tickTask =
        getContext()
            .getSystem()
            .scheduler()
            .scheduleWithFixedDelay(
                interval, interval, getSelf(), "tick", getContext().getDispatcher(), null);
  }

  // subscribe to cluster changes, MemberEvent
  @Override
  public void preStart() {
    cluster.subscribe(getSelf(), MemberEvent.class, ReachabilityEvent.class);
  }

  // re-subscribe when restart
  @Override
  public void postStop() {
    cluster.unsubscribe(getSelf());
    tickTask.cancel();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .matchEquals(
            "tick",
            x -> !nodes.isEmpty(),
            x -> {
              // just pick any one
              List<Address> nodesList = new ArrayList<Address>(nodes);
              Address address =
                  nodesList.get(ThreadLocalRandom.current().nextInt(nodesList.size()));
              ActorSelection service = getContext().actorSelection(address + servicePath);
              service.tell(new StatsJob("this is the text that will be analyzed"), getSelf());
            })
        .match(StatsResult.class, System.out::println)
        .match(JobFailed.class, System.out::println)
        .match(
            CurrentClusterState.class,
            state -> {
              nodes.clear();
              for (Member member : state.getMembers()) {
                if (member.hasRole("compute") && member.status().equals(MemberStatus.up())) {
                  nodes.add(member.address());
                }
              }
            })
        .match(
            MemberUp.class,
            mUp -> {
              if (mUp.member().hasRole("compute")) nodes.add(mUp.member().address());
            })
        .match(
            MemberEvent.class,
            event -> {
              nodes.remove(event.member().address());
            })
        .match(
            UnreachableMember.class,
            unreachable -> {
              nodes.remove(unreachable.member().address());
            })
        .match(
            ReachableMember.class,
            reachable -> {
              if (reachable.member().hasRole("compute")) nodes.add(reachable.member().address());
            })
        .build();
  }
}
