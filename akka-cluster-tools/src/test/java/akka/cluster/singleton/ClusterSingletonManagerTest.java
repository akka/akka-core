/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

@SuppressWarnings("deprecation")
public class ClusterSingletonManagerTest {

  @SuppressWarnings("null")
  public void demo() {
    final ActorSystem system = null;
    final ActorRef queue = null;
    final ActorRef testActor = null;

    // #create-singleton-manager
    final ClusterSingletonManagerSettings settings =
        ClusterSingletonManagerSettings.create(system).withRole("worker");

    system.actorOf(
        ClusterSingletonManager.props(
            Props.create(Consumer.class, () -> new Consumer(queue, testActor)),
            TestSingletonMessages.end(),
            settings),
        "consumer");
    // #create-singleton-manager

    // #create-singleton-proxy
    ClusterSingletonProxySettings proxySettings =
        ClusterSingletonProxySettings.create(system).withRole("worker");

    ActorRef proxy =
        system.actorOf(
            ClusterSingletonProxy.props("/user/consumer", proxySettings), "consumerProxy");
    // #create-singleton-proxy

    // #create-singleton-proxy-dc
    ActorRef proxyDcB =
        system.actorOf(
            ClusterSingletonProxy.props(
                "/user/consumer",
                ClusterSingletonProxySettings.create(system)
                    .withRole("worker")
                    .withDataCenter("B")),
            "consumerProxyDcB");
    // #create-singleton-proxy-dc
  }
}
