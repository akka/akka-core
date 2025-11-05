/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl;

import static akka.actor.typed.javadsl.Behaviors.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import akka.actor.ActorSystem;
import akka.actor.SupervisorStrategy;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Signal;
import akka.actor.typed.Terminated;
import akka.actor.typed.internal.adapter.SchedulerAdapter;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class AdapterTest extends JUnitSuite {

  static akka.actor.Props classic1() {
    return akka.actor.Props.create(Classic1.class, () -> new Classic1());
  }

  static class Classic1 extends akka.actor.AbstractActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals("ping", s -> getSender().tell("pong", getSelf()))
          .match(
              ThrowIt.class,
              t -> {
                throw t;
              })
          .build();
    }
  }

  static class Typed1 {
    private final akka.actor.ActorRef ref;
    private final akka.actor.ActorRef probe;

    private Typed1(akka.actor.ActorRef ref, akka.actor.ActorRef probe) {
      this.ref = ref;
      this.probe = probe;
    }

    static Behavior<String> create(akka.actor.ActorRef ref, akka.actor.ActorRef probe) {
      Typed1 logic = new Typed1(ref, probe);
      return receive(logic::onMessage, logic::onSignal);
    }

    Behavior<String> onMessage(ActorContext<String> context, String message) {
      if (message.equals("send")) {
        akka.actor.ActorRef replyTo = Adapter.toClassic(context.getSelf());
        ref.tell("ping", replyTo);
        return same();
      } else if (message.equals("pong")) {
        probe.tell("ok", akka.actor.ActorRef.noSender());
        return same();
      } else if (message.equals("actorOf")) {
        akka.actor.ActorRef child = Adapter.actorOf(context, classic1());
        child.tell("ping", Adapter.toClassic(context.getSelf()));
        return same();
      } else if (message.equals("watch")) {
        Adapter.watch(context, ref);
        return same();
      } else if (message.equals("supervise-restart")) {
        // restart is the default, otherwise an intermediate is required
        akka.actor.ActorRef child = Adapter.actorOf(context, classic1());
        Adapter.watch(context, child);
        child.tell(new ThrowIt3(), Adapter.toClassic(context.getSelf()));
        child.tell("ping", Adapter.toClassic(context.getSelf()));
        return same();
      } else if (message.equals("stop-child")) {
        akka.actor.ActorRef child = Adapter.actorOf(context, classic1());
        Adapter.watch(context, child);
        Adapter.stop(context, child);
        return same();
      } else if (message.equals("stop-self")) {
        try {
          context.stop(context.getSelf());
        } catch (Exception e) {
          probe.tell(e, akka.actor.ActorRef.noSender());
        }
        return same();
      } else {
        return unhandled();
      }
    }

    Behavior<String> onSignal(ActorContext<String> context, Signal sig) {
      if (sig instanceof Terminated) {
        probe.tell("terminated", akka.actor.ActorRef.noSender());
        return same();
      } else {
        return unhandled();
      }
    }
  }

  static interface Typed2Msg {};

  static final class Ping implements Typed2Msg {
    public final ActorRef<String> replyTo;

    public Ping(ActorRef<String> replyTo) {
      this.replyTo = replyTo;
    }
  }

  static final class StopIt implements Typed2Msg {}

  abstract static class ThrowIt extends RuntimeException implements Typed2Msg {}

  static class ThrowIt1 extends ThrowIt {}

  static class ThrowIt2 extends ThrowIt {}

  static class ThrowIt3 extends ThrowIt {}

  static akka.actor.Props classic2(ActorRef<Ping> ref, akka.actor.ActorRef probe) {
    return akka.actor.Props.create(Classic2.class, () -> new Classic2(ref, probe));
  }

  static class Classic2 extends akka.actor.AbstractActor {
    private final ActorRef<Ping> ref;
    private final akka.actor.ActorRef probe;
    private final SupervisorStrategy strategy;

    Classic2(ActorRef<Ping> ref, akka.actor.ActorRef probe) {
      this.ref = ref;
      this.probe = probe;
      this.strategy = strategy();
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "send",
              s -> {
                ActorRef<String> replyTo = Adapter.toTyped(getSelf());
                ref.tell(new Ping(replyTo));
              })
          .matchEquals("pong", s -> probe.tell("ok", getSelf()))
          .matchEquals(
              "spawn",
              s -> {
                ActorRef<Typed2Msg> child = Adapter.spawnAnonymous(getContext(), typed2());
                child.tell(new Ping(Adapter.toTyped(getSelf())));
              })
          .matchEquals(
              "actorOf-props",
              s -> {
                // this is how Cluster Sharding can be used
                akka.actor.ActorRef child = getContext().actorOf(typed2Props());
                child.tell(new Ping(Adapter.toTyped(getSelf())), akka.actor.ActorRef.noSender());
              })
          .matchEquals("watch", s -> Adapter.watch(getContext(), ref))
          .match(akka.actor.Terminated.class, t -> probe.tell("terminated", getSelf()))
          .matchEquals("supervise-stop", s -> testSupervice(new ThrowIt1()))
          .matchEquals("supervise-resume", s -> testSupervice(new ThrowIt2()))
          .matchEquals("supervise-restart", s -> testSupervice(new ThrowIt3()))
          .matchEquals(
              "stop-child",
              s -> {
                ActorRef<Typed2Msg> child = Adapter.spawnAnonymous(getContext(), typed2());
                Adapter.watch(getContext(), child);
                Adapter.stop(getContext(), child);
              })
          .build();
    }

    private void testSupervice(ThrowIt t) {
      ActorRef<Typed2Msg> child = Adapter.spawnAnonymous(getContext(), typed2());
      Adapter.watch(getContext(), child);
      child.tell(t);
      child.tell(new Ping(Adapter.toTyped(getSelf())));
    }

    private SupervisorStrategy strategy() {
      return new akka.actor.OneForOneStrategy(
          false,
          akka.japi.pf.DeciderBuilder.match(
                  ThrowIt1.class,
                  e -> {
                    probe.tell("thrown-stop", getSelf());
                    return SupervisorStrategy.stop();
                  })
              .match(
                  ThrowIt2.class,
                  e -> {
                    probe.tell("thrown-resume", getSelf());
                    return SupervisorStrategy.resume();
                  })
              .match(
                  ThrowIt3.class,
                  e -> {
                    probe.tell("thrown-restart", getSelf());
                    // TODO Restart will not really restart the behavior
                    return SupervisorStrategy.restart();
                  })
              .build());
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
      return strategy;
    }
  }

  static Behavior<Typed2Msg> typed2() {
    return Behaviors.receive(
        (context, message) -> {
          if (message instanceof Ping) {
            ActorRef<String> replyTo = ((Ping) message).replyTo;
            replyTo.tell("pong");
            return same();
          } else if (message instanceof StopIt) {
            return stopped();
          } else if (message instanceof ThrowIt) {
            throw (ThrowIt) message;
          } else {
            return unhandled();
          }
        });
  }

  static akka.actor.Props typed2Props() {
    return Adapter.props(() -> typed2());
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("ActorSelectionTest", AkkaSpec.testConf());

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  private final ActorSystem system = actorSystemResource.getSystem();

  @Test
  public void shouldSendMessageFromTypedToClassic() {
    TestKit probe = new TestKit(system);
    akka.actor.ActorRef classicRef = system.actorOf(classic1());
    ActorRef<String> typedRef =
        Adapter.spawnAnonymous(system, Typed1.create(classicRef, probe.getRef()));
    typedRef.tell("send");
    probe.expectMsg("ok");
  }

  @Test
  public void shouldSendMessageFromClassicToTyped() {
    TestKit probe = new TestKit(system);
    ActorRef<Ping> typedRef = Adapter.spawnAnonymous(system, typed2()).narrow();
    akka.actor.ActorRef classicRef = system.actorOf(classic2(typedRef, probe.getRef()));
    classicRef.tell("send", akka.actor.ActorRef.noSender());
    probe.expectMsg("ok");
  }

  @Test
  public void shouldSpawnTypedChildFromClassicParent() {
    TestKit probe = new TestKit(system);
    ActorRef<Ping> ignore = Adapter.spawnAnonymous(system, ignore());
    akka.actor.ActorRef classicRef = system.actorOf(classic2(ignore, probe.getRef()));
    classicRef.tell("spawn", akka.actor.ActorRef.noSender());
    probe.expectMsg("ok");
  }

  @Test
  public void shouldActorOfTypedChildViaPropsFromClassicParent() {
    TestKit probe = new TestKit(system);
    ActorRef<Ping> ignore = Adapter.spawnAnonymous(system, ignore());
    akka.actor.ActorRef classicRef = system.actorOf(classic2(ignore, probe.getRef()));
    classicRef.tell("actorOf-props", akka.actor.ActorRef.noSender());
    probe.expectMsg("ok");
  }

  @Test
  public void shouldActorOfClassicChildFromTypedParent() {
    TestKit probe = new TestKit(system);
    akka.actor.ActorRef ignore = system.actorOf(akka.actor.Props.empty());
    ActorRef<String> typedRef =
        Adapter.spawnAnonymous(system, Typed1.create(ignore, probe.getRef()));
    typedRef.tell("actorOf");
    probe.expectMsg("ok");
  }

  @Test
  public void shouldWatchTypedFromClassic() {
    TestKit probe = new TestKit(system);
    ActorRef<Typed2Msg> typedRef = Adapter.spawnAnonymous(system, typed2());
    ActorRef<Ping> typedRef2 = typedRef.narrow();
    akka.actor.ActorRef classicRef = system.actorOf(classic2(typedRef2, probe.getRef()));
    classicRef.tell("watch", akka.actor.ActorRef.noSender());
    typedRef.tell(new StopIt());
    probe.expectMsg("terminated");
  }

  @Test
  public void shouldWatchClassicFromTyped() {
    TestKit probe = new TestKit(system);
    akka.actor.ActorRef classicRef = system.actorOf(classic1());
    ActorRef<String> typedRef =
        Adapter.spawnAnonymous(system, Typed1.create(classicRef, probe.getRef()));
    typedRef.tell("watch");
    classicRef.tell(akka.actor.PoisonPill.getInstance(), akka.actor.ActorRef.noSender());
    probe.expectMsg("terminated");
  }

  @Test
  public void shouldSuperviseClassicChildAsRestartFromTypedParent() {
    TestKit probe = new TestKit(system);
    akka.actor.ActorRef ignore = system.actorOf(akka.actor.Props.empty());
    ActorRef<String> typedRef =
        Adapter.spawnAnonymous(system, Typed1.create(ignore, probe.getRef()));

    int originalLogLevel = system.getEventStream().logLevel();
    try {
      // suppress the logging with stack trace
      system.getEventStream().setLogLevel(Integer.MIN_VALUE); // OFF

      typedRef.tell("supervise-restart");
      probe.expectMsg("ok");
    } finally {
      system.getEventStream().setLogLevel(originalLogLevel);
    }
    probe.expectNoMessage(Duration.ofMillis(100)); // no pong
  }

  @Test
  public void shouldStopTypedChildFromClassicParent() {
    TestKit probe = new TestKit(system);
    ActorRef<Ping> ignore = Adapter.spawnAnonymous(system, ignore());
    akka.actor.ActorRef classicRef = system.actorOf(classic2(ignore, probe.getRef()));
    classicRef.tell("stop-child", akka.actor.ActorRef.noSender());
    probe.expectMsg("terminated");
  }

  @Test
  public void shouldStopClassicChildFromTypedParent() {
    TestKit probe = new TestKit(system);
    akka.actor.ActorRef ignore = system.actorOf(akka.actor.Props.empty());
    ActorRef<String> typedRef =
        Adapter.spawnAnonymous(system, Typed1.create(ignore, probe.getRef()));
    typedRef.tell("stop-child");
    probe.expectMsg("terminated");
  }

  @Test
  public void stopSelfWillCauseError() {
    TestKit probe = new TestKit(system);
    akka.actor.ActorRef ignore = system.actorOf(akka.actor.Props.empty());
    ActorRef<String> typedRef =
        Adapter.spawnAnonymous(system, Typed1.create(ignore, probe.getRef()));
    typedRef.tell("stop-self");
    probe.expectMsgClass(IllegalArgumentException.class);
  }

  @Test
  public void shouldConvertScheduler() {
    akka.actor.typed.Scheduler typedScheduler = Adapter.toTyped(system.scheduler());
    assertEquals(SchedulerAdapter.class, typedScheduler.getClass());
    akka.actor.Scheduler classicScheduler = Adapter.toClassic(typedScheduler);
    assertSame(system.scheduler(), classicScheduler);
  }
}
