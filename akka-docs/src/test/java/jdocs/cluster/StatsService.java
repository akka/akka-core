/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.routing.ClusterRouterGroup;
import akka.cluster.routing.ClusterRouterGroupSettings;
import akka.cluster.routing.ClusterRouterPool;
import akka.cluster.routing.ClusterRouterPoolSettings;
import akka.routing.ConsistentHashingGroup;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope;
import akka.routing.FromConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import jdocs.cluster.StatsMessages.StatsJob;

// #service
public class StatsService extends AbstractActor {

  // This router is used both with lookup and deploy of routees. If you
  // have a router with only lookup of routees you can use Props.empty()
  // instead of Props.create(StatsWorker.class).
  ActorRef workerRouter =
      getContext()
          .actorOf(FromConfig.getInstance().props(Props.create(StatsWorker.class)), "workerRouter");

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            StatsJob.class,
            job -> !job.getText().isEmpty(),
            job -> {
              String[] words = job.getText().split(" ");
              ActorRef replyTo = getSender();

              // create actor that collects replies from workers
              ActorRef aggregator =
                  getContext().actorOf(Props.create(StatsAggregator.class, words.length, replyTo));

              // send each word to a worker
              for (String word : words) {
                workerRouter.tell(new ConsistentHashableEnvelope(word, word), aggregator);
              }
            })
        .build();
  }
}
// #service

// not used, only for documentation
abstract class StatsService2 extends AbstractActor {
  // #router-lookup-in-code
  int totalInstances = 100;
  Iterable<String> routeesPaths = Collections.singletonList("/user/statsWorker");
  boolean allowLocalRoutees = true;
  Set<String> useRoles = new HashSet<>(Arrays.asList("compute"));
  ActorRef workerRouter =
      getContext()
          .actorOf(
              new ClusterRouterGroup(
                      new ConsistentHashingGroup(routeesPaths),
                      new ClusterRouterGroupSettings(
                          totalInstances, routeesPaths, allowLocalRoutees, useRoles))
                  .props(),
              "workerRouter2");
  // #router-lookup-in-code
}

// not used, only for documentation
abstract class StatsService3 extends AbstractActor {
  // #router-deploy-in-code
  int totalInstances = 100;
  int maxInstancesPerNode = 3;
  boolean allowLocalRoutees = false;
  Set<String> useRoles = new HashSet<>(Arrays.asList("compute"));
  ActorRef workerRouter =
      getContext()
          .actorOf(
              new ClusterRouterPool(
                      new ConsistentHashingPool(0),
                      new ClusterRouterPoolSettings(
                          totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles))
                  .props(Props.create(StatsWorker.class)),
              "workerRouter3");
  // #router-deploy-in-code
}
