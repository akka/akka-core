/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl;

import static akka.actor.typed.javadsl.Behaviors.same;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

/** Test creating [[MutableActor]]s using [[ReceiveBuilder]] */
public class ReceiveBuilderTest extends JUnitSuite {

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void testMutableCounter() {
    Behavior<BehaviorBuilderTest.CounterMessage> mutable =
        Behaviors.setup(
            context ->
                new AbstractBehavior<BehaviorBuilderTest.CounterMessage>(context) {
                  int currentValue = 0;

                  private Behavior<BehaviorBuilderTest.CounterMessage> receiveIncrease(
                      BehaviorBuilderTest.Increase message) {
                    currentValue++;
                    return this;
                  }

                  private Behavior<BehaviorBuilderTest.CounterMessage> receiveGet(
                      BehaviorBuilderTest.Get get) {
                    get.sender.tell(new BehaviorBuilderTest.Got(currentValue));
                    return this;
                  }

                  @Override
                  public Receive<BehaviorBuilderTest.CounterMessage> createReceive() {
                    return newReceiveBuilder()
                        .onMessage(BehaviorBuilderTest.Increase.class, this::receiveIncrease)
                        .onMessage(BehaviorBuilderTest.Get.class, this::receiveGet)
                        .build();
                  }
                });
  }

  @Test
  public void caseSelectedInOrderAdded() {
    final TestProbe<Object> probe = testKit.createTestProbe();
    Behavior<Object> behavior =
        ReceiveBuilder.<Object>create()
            .onMessage(
                String.class,
                msg -> {
                  probe.ref().tell("handler 1: " + msg);
                  return Behaviors.same();
                })
            .onMessage(
                String.class,
                msg -> {
                  probe.ref().tell("handler 2: " + msg);
                  return Behaviors.same();
                })
            .build();
    ActorRef<Object> ref = testKit.spawn(behavior);
    ref.tell("message");
    probe.expectMessage("handler 1: message");
  }

  @Test
  public void applyPredicate() {
    final TestProbe<Object> probe = testKit.createTestProbe();
    Behavior<Object> behavior =
        ReceiveBuilder.create()
            .onMessage(
                String.class,
                msg -> "other".equals(msg),
                msg -> {
                  probe.ref().tell("handler 1: " + msg);
                  return Behaviors.same();
                })
            .onMessage(
                String.class,
                msg -> {
                  probe.ref().tell("handler 2: " + msg);
                  return Behaviors.same();
                })
            .build();
    ActorRef<Object> ref = testKit.spawn(behavior);
    ref.tell("message");
    probe.expectMessage("handler 2: message");
  }

  @Test
  public void catchAny() {
    final TestProbe<Object> probe = testKit.createTestProbe();
    Behavior<Object> behavior =
        ReceiveBuilder.create()
            .onAnyMessage(
                msg -> {
                  probe.ref().tell(msg);
                  return same();
                })
            .build();
    ActorRef<Object> ref = testKit.spawn(behavior);
    ref.tell("message");
    probe.expectMessage("message");
  }

  public void compileOnlyHandlerAllowedToThrowCheckedException() {
    Behavior<Object> behavior =
        ReceiveBuilder.create()
            .onMessageEquals(
                "exactly",
                () -> {
                  throw new Exception("checked");
                })
            .onMessage(
                String.class,
                msg -> {
                  throw new Exception("checked");
                })
            .onMessage(
                Integer.class,
                msg -> true,
                msg -> {
                  throw new Exception("checked");
                })
            .onSignalEquals(
                PostStop.instance(),
                () -> {
                  throw new Exception("checked");
                })
            .onSignal(
                PostStop.class,
                (signal) -> {
                  throw new Exception("checked");
                })
            .onSignal(
                PostStop.class,
                signal -> true,
                signal -> {
                  throw new Exception("checked");
                })
            .build();
  }
}
