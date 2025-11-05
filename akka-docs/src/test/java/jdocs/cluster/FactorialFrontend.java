/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.cluster.metrics.AdaptiveLoadBalancingGroup;
import akka.cluster.metrics.AdaptiveLoadBalancingPool;
import akka.cluster.metrics.HeapMetricsSelector;
import akka.cluster.metrics.SystemLoadAverageMetricsSelector;
import akka.cluster.routing.ClusterRouterGroup;
import akka.cluster.routing.ClusterRouterGroupSettings;
import akka.cluster.routing.ClusterRouterPool;
import akka.cluster.routing.ClusterRouterPoolSettings;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.FromConfig;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

// #frontend
public class FactorialFrontend extends AbstractActor {
  final int upToN;
  final boolean repeat;

  LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  ActorRef backend =
      getContext().actorOf(FromConfig.getInstance().props(), "factorialBackendRouter");

  public FactorialFrontend(int upToN, boolean repeat) {
    this.upToN = upToN;
    this.repeat = repeat;
  }

  @Override
  public void preStart() {
    sendJobs();
    getContext().setReceiveTimeout(Duration.ofSeconds(10));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            FactorialResult.class,
            result -> {
              if (result.n == upToN) {
                log.debug("{}! = {}", result.n, result.factorial);
                if (repeat) sendJobs();
                else getContext().stop(getSelf());
              }
            })
        .match(
            ReceiveTimeout.class,
            x -> {
              log.info("Timeout");
              sendJobs();
            })
        .build();
  }

  void sendJobs() {
    log.info("Starting batch of factorials up to [{}]", upToN);
    for (int n = 1; n <= upToN; n++) {
      backend.tell(n, getSelf());
    }
  }
}
// #frontend

// not used, only for documentation
abstract class FactorialFrontend2 extends AbstractActor {
  // #router-lookup-in-code
  int totalInstances = 100;
  Iterable<String> routeesPaths = Arrays.asList("/user/factorialBackend", "");
  boolean allowLocalRoutees = true;
  Set<String> useRoles = new HashSet<>(Arrays.asList("backend"));
  ActorRef backend =
      getContext()
          .actorOf(
              new ClusterRouterGroup(
                      new AdaptiveLoadBalancingGroup(
                          HeapMetricsSelector.getInstance(), Collections.<String>emptyList()),
                      new ClusterRouterGroupSettings(
                          totalInstances, routeesPaths, allowLocalRoutees, useRoles))
                  .props(),
              "factorialBackendRouter2");

  // #router-lookup-in-code
}

// not used, only for documentation
abstract class FactorialFrontend3 extends AbstractActor {
  // #router-deploy-in-code
  int totalInstances = 100;
  int maxInstancesPerNode = 3;
  boolean allowLocalRoutees = false;
  Set<String> useRoles = new HashSet<>(Arrays.asList("backend"));
  ActorRef backend =
      getContext()
          .actorOf(
              new ClusterRouterPool(
                      new AdaptiveLoadBalancingPool(
                          SystemLoadAverageMetricsSelector.getInstance(), 0),
                      new ClusterRouterPoolSettings(
                          totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles))
                  .props(Props.create(FactorialBackend.class)),
              "factorialBackendRouter3");
  // #router-deploy-in-code
}
