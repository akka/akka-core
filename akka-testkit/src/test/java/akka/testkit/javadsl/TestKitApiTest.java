/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.javadsl;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.testkit.TestActor;
import java.time.Duration;
import java.util.List;

public class TestKitApiTest {

  public void compileOnlyTestCase() {
    TestKit testKit = null;
    ActorRef actorRef = null;
    TestActor.AutoPilot autoPilot = null;

    // just coverage of calling all testkit methods, actual impl is tested elsewhere
    testKit.awaitAssert(
        () -> {
          return null;
        });
    testKit.awaitAssert(
        Duration.ofSeconds(3),
        () -> {
          return null;
        });
    testKit.awaitAssert(
        Duration.ofSeconds(3),
        Duration.ofMillis(200),
        () -> {
          return null;
        });

    testKit.awaitCond(
        () -> {
          return true;
        });
    testKit.awaitCond(
        Duration.ofSeconds(3),
        () -> {
          return true;
        });
    testKit.awaitCond(
        Duration.ofSeconds(3),
        Duration.ofMillis(200),
        () -> {
          return true;
        });
    testKit.awaitCond(
        Duration.ofSeconds(3),
        Duration.ofMillis(200),
        "details",
        () -> {
          return true;
        });

    testKit.childActorOf(Props.empty());
    testKit.childActorOf(Props.empty(), "name");
    testKit.childActorOf(Props.empty(), SupervisorStrategy.defaultStrategy());
    testKit.childActorOf(Props.empty(), "name", SupervisorStrategy.defaultStrategy());

    testKit.dilated(Duration.ofSeconds(3));

    testKit.expectMsg("message");
    testKit.expectMsg(Duration.ofSeconds(3), "message");
    testKit.expectMsg(Duration.ofSeconds(3), "message", "hint");

    testKit.expectMsgClass(String.class);
    testKit.expectMsgClass(Duration.ofSeconds(3), String.class);

    testKit.expectMsgEquals("message");
    testKit.expectMsgEquals(Duration.ofSeconds(3), "message");

    // FIXME why are these called PF when they take a total function, how are they supposed to be
    // used? #27562
    testKit.expectMsgPF(
        Duration.ofSeconds(3),
        "hint",
        (value) -> {
          return null;
        });
    testKit.expectMsgPF(
        "hint",
        (value) -> {
          return null;
        });

    testKit.expectNoMessage();
    testKit.expectNoMessage(Duration.ofSeconds(3));

    testKit.expectTerminated(actorRef);
    testKit.expectTerminated(Duration.ofSeconds(3), actorRef);

    // FIXME how is this supposed to be used, scaladoc talks about a partial function but it accepts
    // a total function #27562
    Object fishResult1 =
        testKit.fishForMessage(
            Duration.ofSeconds(3),
            "hint",
            (message) -> {
              return "fishResult1";
            });
    // FIXME how is this supposed to be used, scaladoc talks about a partial function but it accepts
    // a total function #27562
    String result =
        testKit.fishForSpecificMessage(
            Duration.ofSeconds(3),
            "hint",
            (message) -> {
              return "fishResult2";
            });

    List<Object> tenMessages = testKit.receiveN(10);
    List<Object> tenMoreMessages = testKit.receiveN(10, Duration.ofSeconds(3));
    Object oneMoreMessage = testKit.receiveOne(Duration.ofSeconds(3));

    // FIXME how is this supposed to be used, scaladoc talks about a partial function but it accepts
    // a total function #27562
    List<Object> receiveWhileResults1 =
        testKit.receiveWhile(
            Duration.ofSeconds(3),
            (message) -> {
              return message;
            });
    // FIXME how is this supposed to be used, scaladoc talks about a partial function but it accepts
    // a total function #27562
    List<Object> receiveWhileResults2 =
        testKit.receiveWhile(
            Duration.ofSeconds(3),
            Duration.ofSeconds(1),
            10,
            (message) -> {
              return "result";
            });

    // FIXME how is this supposed to be used, scaladoc talks about a partial function but it accepts
    // a total function #27562
    testKit.ignoreMsg(
        (message) -> {
          return message;
        });
    testKit.ignoreNoMsg();

    testKit.expectMsgAllOf("one", "two");
    testKit.expectMsgAllOfWithin(Duration.ofSeconds(3), "one", "two");
    testKit.expectMsgAnyOf("one", "two");
    testKit.expectMsgAnyOfWithin(Duration.ofSeconds(3), "one", "two");
    testKit.expectMsgAnyClassOf(String.class, Integer.class);
    testKit.expectMsgAnyClassOf(Duration.ofSeconds(3), String.class, Integer.class);

    ActorRef lastSender = testKit.getLastSender();
    ActorSystem system = testKit.getSystem();
    Duration remaining = testKit.getRemaining();
    Duration remainingOrDefault = testKit.getRemainingOrDefault();
    Duration dilated = testKit.dilated(Duration.ofSeconds(3));
    ActorRef testActor = testKit.getTestActor();
    boolean msgAvailable = testKit.msgAvailable();

    testKit.forward(actorRef);
    testKit.send(actorRef, "message");
    testKit.watch(actorRef);
    testKit.unwatch(actorRef);

    testKit.setAutoPilot(autoPilot);
  }
}
