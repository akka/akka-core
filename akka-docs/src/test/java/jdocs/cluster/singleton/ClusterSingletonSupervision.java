/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster.singleton;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;

// #singleton-supervisor-actor-usage-imports
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
// #singleton-supervisor-actor-usage-imports

abstract class ClusterSingletonSupervision extends AbstractActor {

  public ActorRef createSingleton(String name, Props props, SupervisorStrategy supervisorStrategy) {
    // #singleton-supervisor-actor-usage
    return getContext()
        .system()
        .actorOf(
            ClusterSingletonManager.props(
                Props.create(
                    SupervisorActor.class, () -> new SupervisorActor(props, supervisorStrategy)),
                PoisonPill.getInstance(),
                ClusterSingletonManagerSettings.create(getContext().system())),
            name = name);
    // #singleton-supervisor-actor-usage
  }
}
