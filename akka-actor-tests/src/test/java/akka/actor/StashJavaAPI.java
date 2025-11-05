/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.TestProbe;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class StashJavaAPI extends JUnitSuite {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("StashJavaAPI", ActorWithBoundedStashSpec.testConf());

  private final ActorSystem system = actorSystemResource.getSystem();

  private void testAStashApi(Props props) {
    ActorRef ref = system.actorOf(props);
    final TestProbe probe = new TestProbe(system);
    probe.send(ref, "Hello");
    probe.send(ref, "Hello2");
    probe.send(ref, "Hello12");
    probe.expectMsg(5);
  }

  @Test
  public void mustBeAbleToUseStash() {
    testAStashApi(Props.create(StashJavaAPITestActors.WithStash.class));
  }

  @Test
  public void mustBeAbleToUseUnboundedStash() {
    testAStashApi(Props.create(StashJavaAPITestActors.WithUnboundedStash.class));
  }

  @Test
  public void mustBeAbleToUseUnrestrictedStash() {
    testAStashApi(
        Props.create(StashJavaAPITestActors.WithUnrestrictedStash.class)
            .withMailbox("akka.actor.mailbox.unbounded-deque-based"));
  }
}
