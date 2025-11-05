/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import static jdocs.akka.typed.BubblingSample.Boss;
import static jdocs.akka.typed.BubblingSample.Protocol;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class BubblingSampleTest extends JUnitSuite {

  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource("akka.loglevel = off");

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void testBubblingSample() throws Exception {
    ActorRef<Protocol.Command> boss = testKit.spawn(Boss.create(), "upper-management");
    TestProbe<String> replyProbe = testKit.createTestProbe(String.class);
    boss.tell(new Protocol.Hello("hi 1", replyProbe.getRef()));
    replyProbe.expectMessage("hi 1");
    boss.tell(new Protocol.Fail("ping"));

    // message may be lost when MiddleManagement is stopped, but eventually it will be functional
    // again
    replyProbe.awaitAssert(
        () -> {
          boss.tell(new Protocol.Hello("hi 2", replyProbe.getRef()));
          replyProbe.expectMessage(Duration.ofMillis(200), "hi 2");
          return null;
        });
  }
}
