/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.typed;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.ClusterSingletonSettings;
import akka.cluster.typed.SingletonActor;
import akka.coordination.lease.LeaseUsageSettings;
import java.time.Duration;

class LeaseDocExample {

  static class PingPong {
    interface Command {}

    static final class Perish implements Command {}

    public static Behavior<Command> create() {
      return null;
    }
  }

  // Note: actual config example is in Scala LeaseDocSpec

  void perSingletonLeaseConfig() {
    ActorSystem<?> system = null;
    // #singleton-load-config
    LeaseUsageSettings leaseSettings =
        LeaseUsageSettings.create(
            system.settings().config().getConfig("my.app.my-singleton-lease"));
    ClusterSingletonSettings settings =
        ClusterSingletonSettings.create(system).withLeaseSettings(leaseSettings);
    SingletonActor<PingPong.Command> singletonActor =
        SingletonActor.of(PingPong.create(), "ping-pong")
            .withStopMessage(new PingPong.Perish())
            .withSettings(settings);
    ClusterSingleton.get(system).init(singletonActor);
    // #singleton-load-config
  }

  void perSingletonSettings() {
    ActorSystem<?> system = null;
    // #singleton-settings
    ClusterSingletonSettings settings =
        ClusterSingletonSettings.create(system)
            .withLeaseSettings(
                LeaseUsageSettings.create(
                    "akka.coordination.lease.kubernetes",
                    Duration.ofSeconds(5),
                    "my-pingpong-singleton-lease"));
    SingletonActor<PingPong.Command> singletonActor =
        SingletonActor.of(PingPong.create(), "ping-pong")
            .withStopMessage(new PingPong.Perish())
            .withSettings(settings);
    // #singleton-settings
  }
}
