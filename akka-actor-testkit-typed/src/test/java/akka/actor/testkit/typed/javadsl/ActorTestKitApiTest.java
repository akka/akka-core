/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Props;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.Behaviors;
import java.time.Duration;
import java.util.List;

public class ActorTestKitApiTest {

  public void compileOnlyTestCase() {
    ActorTestKit testKit = null;

    TestProbe<String> probe1 = testKit.createTestProbe();
    TestProbe<String> probe2 = testKit.createTestProbe("name");
    TestProbe<Integer> probe3 = testKit.createTestProbe(Integer.class);
    TestProbe<Integer> probe4 = testKit.createTestProbe("name", Integer.class);

    Scheduler scheduler = testKit.scheduler();
    SerializationTestKit serializationTestKit = testKit.serializationTestKit();
    testKit.shutdownTestKit();

    testKit.spawn(Behaviors.empty());
    testKit.spawn(Behaviors.empty(), "name");
    testKit.spawn(Behaviors.empty(), Props.empty());
    testKit.spawn(Behaviors.empty(), "name", Props.empty());

    ActorRef<String> actorRef = null;
    testKit.stop(actorRef);
    testKit.stop(actorRef, Duration.ofSeconds(3));
  }

  public void testProbeCompileOnlyTestCase() {
    TestProbe<String> probe = null;

    String awaitAssertSupplied1 =
        probe.awaitAssert(
            () -> {
              return "supplied";
            });
    String awaitAssertSupplied2 =
        probe.awaitAssert(
            Duration.ofSeconds(3),
            () -> {
              return "supplied";
            });
    String awaitAssertSupplied3 =
        probe.awaitAssert(
            Duration.ofSeconds(3),
            Duration.ofMillis(200),
            () -> {
              return "supplied";
            });

    String expectMessage1 = probe.expectMessage("message-1");
    String expectMessage2 = probe.expectMessage(Duration.ofSeconds(3), "message-2");
    String expectMessage3 = probe.expectMessage(Duration.ofSeconds(3), "hint", "message-2");

    String receiveMessage1 = probe.receiveMessage();
    String receiveMessage2 = probe.receiveMessage(Duration.ofSeconds(3));

    probe.expectMessageClass(String.class);
    probe.expectMessageClass(String.class, Duration.ofSeconds(3));

    List<String> fishedMessages1 =
        probe.fishForMessage(
            Duration.ofSeconds(3),
            (message) -> {
              return FishingOutcomes.complete();
            });
    List<String> fishedMessages2 =
        probe.fishForMessage(
            Duration.ofSeconds(3),
            "hint",
            (message) -> {
              return FishingOutcomes.complete();
            });

    probe.expectNoMessage();
    probe.expectNoMessage(Duration.ofSeconds(3));

    List<String> tenMessages1 = probe.receiveSeveralMessages(10);
    List<String> tenMessages2 = probe.receiveSeveralMessages(10, Duration.ofSeconds(3));

    ActorRef<String> ref = probe.getRef();
    Duration remaining = probe.getRemaining();
    Duration remainingOr = probe.getRemainingOr(Duration.ofSeconds(3));
    Duration remainingOrDefault = probe.getRemainingOrDefault();

    ActorRef<Object> actorRef = null;
    probe.expectTerminated(actorRef);
    probe.expectTerminated(actorRef, Duration.ofSeconds(3));
  }
}
