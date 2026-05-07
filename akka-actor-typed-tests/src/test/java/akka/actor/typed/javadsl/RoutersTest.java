/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.receptionist.ServiceKey;
import akka.testkit.AkkaSpec;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class RoutersTest extends JUnitSuite {

  @ClassRule
  public static final TestKitJunitResource testKit = new TestKitJunitResource(AkkaSpec.testConf());

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  public void compileOnlyApiTest() {

    final ServiceKey<String> key = ServiceKey.create(String.class, "key");
    Behavior<String> group = Routers.group(key).withRandomRouting().withRoundRobinRouting();

    Behavior<String> pool =
        Routers.pool(5, Behaviors.<String>empty()).withRandomRouting().withRoundRobinRouting();
  }

  @Test
  public void poolBroadcastTest() {
    TestProbe<String> probe = testKit.createTestProbe();
    Behavior<String> behavior =
        Behaviors.receiveMessage(
            (String str) -> {
              probe.getRef().tell(str);
              return Behaviors.same();
            });

    Behavior<String> poolBehavior =
        Routers.pool(4, behavior).withBroadcastPredicate(str -> str.startsWith("bc-"));

    ActorRef<String> pool = testKit.spawn(poolBehavior);

    String notBroadcastMsg = "not-bc-message";
    pool.tell(notBroadcastMsg);

    String broadcastMsg = "bc-message";
    pool.tell(broadcastMsg);

    List<String> messages = probe.receiveSeveralMessages(5);
    assertTrue("non-broadcast message arrives", messages.contains(notBroadcastMsg));

    int broadcast = 0;
    for (String msg : messages) if (msg == broadcastMsg) broadcast++;
    assertEquals("broadcast message arrives 4 times", broadcast, 4);
  }
}
