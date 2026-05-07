/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed;

import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorSystem;
import akka.cluster.ClusterEvent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ClusterApiTest extends JUnitSuite {

  @Test
  public void joinLeaveAndObserve() throws Exception {
    Config config =
        ConfigFactory.parseString(
            "akka.actor.provider = cluster \n"
                + "akka.remote.artery.canonical.port = 0 \n"
                + "akka.remote.artery.canonical.hostname = 127.0.0.1 \n"
                + "akka.cluster.jmx.multi-mbeans-in-same-jvm = on \n"
                + "akka.coordinated-shutdown.terminate-actor-system = off \n"
                + "akka.coordinated-shutdown.run-by-actor-system-terminate = off \n");

    ActorSystem<?> system1 =
        ActorSystem.wrap(akka.actor.ActorSystem.create("ClusterApiTest", config));
    ActorSystem<?> system2 =
        ActorSystem.wrap(akka.actor.ActorSystem.create("ClusterApiTest", config));

    try {
      Cluster cluster1 = Cluster.get(system1);
      Cluster cluster2 = Cluster.get(system2);

      TestProbe<ClusterEvent.ClusterDomainEvent> probe1 = TestProbe.create(system1);

      cluster1.subscriptions().tell(new Subscribe<>(probe1.ref().narrow(), SelfUp.class));
      cluster1.manager().tell(new Join(cluster1.selfMember().address()));
      probe1.expectMessageClass(SelfUp.class);

      TestProbe<ClusterEvent.ClusterDomainEvent> probe2 = TestProbe.create(system2);
      cluster2.subscriptions().tell(new Subscribe<>(probe2.ref().narrow(), SelfUp.class));
      cluster2.manager().tell(new Join(cluster1.selfMember().address()));
      probe2.expectMessageClass(SelfUp.class);

      cluster2.subscriptions().tell(new Subscribe<>(probe2.ref().narrow(), SelfRemoved.class));
      cluster2.manager().tell(new Leave(cluster2.selfMember().address()));

      probe2.expectMessageClass(SelfRemoved.class);
    } finally {
      system1.terminate();
      system1.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
      system2.terminate();
      system2.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
  }
}
