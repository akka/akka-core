/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import static jdocs.cluster.TransformationMessages.BACKEND_REGISTRATION;

import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import jdocs.cluster.TransformationMessages.TransformationJob;
import jdocs.cluster.TransformationMessages.TransformationResult;

// #backend
public class TransformationBackend extends AbstractActor {

  Cluster cluster = Cluster.get(getContext().getSystem());

  // subscribe to cluster changes, MemberUp
  @Override
  public void preStart() {
    cluster.subscribe(getSelf(), MemberUp.class);
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
            TransformationJob.class,
            job -> {
              getSender().tell(new TransformationResult(job.getText().toUpperCase()), getSelf());
            })
        .match(
            CurrentClusterState.class,
            state -> {
              for (Member member : state.getMembers()) {
                if (member.status().equals(MemberStatus.up())) {
                  register(member);
                }
              }
            })
        .match(
            MemberUp.class,
            mUp -> {
              register(mUp.member());
            })
        .build();
  }

  void register(Member member) {
    if (member.hasRole("frontend"))
      getContext()
          .actorSelection(member.address() + "/user/frontend")
          .tell(BACKEND_REGISTRATION, getSelf());
  }
}
// #backend
