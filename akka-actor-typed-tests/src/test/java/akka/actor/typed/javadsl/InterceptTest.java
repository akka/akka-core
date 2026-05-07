/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.*;
import akka.testkit.AkkaSpec;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class InterceptTest extends JUnitSuite {

  @ClassRule
  public static final TestKitJunitResource testKit = new TestKitJunitResource(AkkaSpec.testConf());

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void interceptMessage() {
    final TestProbe<String> interceptProbe = testKit.createTestProbe();
    BehaviorInterceptor<String, String> interceptor =
        new BehaviorInterceptor<String, String>(String.class) {
          @Override
          public Behavior<String> aroundReceive(
              TypedActorContext<String> ctx, String msg, ReceiveTarget<String> target) {
            interceptProbe.getRef().tell(msg);
            return target.apply(ctx, msg);
          }

          @Override
          public Behavior<String> aroundSignal(
              TypedActorContext<String> ctx, Signal signal, SignalTarget<String> target) {
            return target.apply(ctx, signal);
          }
        };

    final TestProbe<String> probe = testKit.createTestProbe();
    ActorRef<String> ref =
        testKit.spawn(
            Behaviors.intercept(
                () -> interceptor,
                Behaviors.receiveMessage(
                    (String msg) -> {
                      probe.getRef().tell(msg);
                      return Behaviors.same();
                    })));
    ref.tell("Hello");

    interceptProbe.expectMessage("Hello");
    probe.expectMessage("Hello");
  }

  interface Message {}

  static class A implements Message {}

  static class B implements Message {}

  @Test
  public void interceptMessageSubclasses() {
    final TestProbe<Message> interceptProbe = testKit.createTestProbe();
    BehaviorInterceptor<Message, Message> interceptor =
        new BehaviorInterceptor<Message, Message>(Message.class) {

          @Override
          public Behavior<Message> aroundReceive(
              TypedActorContext<Message> ctx, Message msg, ReceiveTarget<Message> target) {
            interceptProbe.getRef().tell(msg);
            return target.apply(ctx, msg);
          }

          @Override
          public Behavior<Message> aroundSignal(
              TypedActorContext<Message> ctx, Signal signal, SignalTarget<Message> target) {
            return target.apply(ctx, signal);
          }
        };

    final TestProbe<Message> probe = testKit.createTestProbe();
    ActorRef<Message> ref =
        testKit.spawn(
            Behaviors.intercept(
                () -> interceptor,
                Behaviors.receiveMessage(
                    (Message msg) -> {
                      probe.getRef().tell(msg);
                      return Behaviors.same();
                    })));
    ref.tell(new A());
    ref.tell(new B());

    interceptProbe.expectMessageClass(A.class);
    probe.expectMessageClass(A.class);
    interceptProbe.expectMessageClass(B.class);
    probe.expectMessageClass(B.class);
  }
}
