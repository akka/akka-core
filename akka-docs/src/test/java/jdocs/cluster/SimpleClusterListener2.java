/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class SimpleClusterListener2 extends AbstractActor {
  LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  // #join
  Cluster cluster = Cluster.get(getContext().getSystem());
  // #join

  // subscribe to cluster changes
  @Override
  public void preStart() {
    // #join
    cluster.join(cluster.selfAddress());
    // #join

    // #subscribe
    cluster.subscribe(getSelf(), MemberEvent.class, UnreachableMember.class);
    // #subscribe

    // #register-on-memberup
    cluster.registerOnMemberUp(
        () -> cluster.subscribe(getSelf(), MemberEvent.class, UnreachableMember.class));
    // #register-on-memberup
  }

  // re-subscribe when restart
  @Override
  public void postStop() {
    cluster.unsubscribe(getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            CurrentClusterState.class,
            state -> {
              log.info("Current members: {}", state.members());
            })
        .match(
            MemberUp.class,
            mUp -> {
              log.info("Member is Up: {}", mUp.member());
            })
        .match(
            UnreachableMember.class,
            mUnreachable -> {
              log.info("Member detected as unreachable: {}", mUnreachable.member());
            })
        .match(
            MemberRemoved.class,
            mRemoved -> {
              log.info("Member is Removed: {}", mRemoved.member());
            })
        .match(
            MemberEvent.class,
            event -> {
              // ignore
            })
        .build();
  }
}
