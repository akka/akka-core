/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed.coexistence;

import akka.actor.AbstractActor;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
// #adapter-import
// In java use the static methods on Adapter to convert from typed to classic
import akka.actor.typed.javadsl.Adapter;
// #adapter-import
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import scala.concurrent.duration.Duration;

import static akka.actor.typed.javadsl.Behaviors.same;

public class ClassicWatchingTypedTest extends JUnitSuite {

  // #classic-watch
  public static class Classic extends AbstractActor {
    public static akka.actor.Props props() {
      return akka.actor.Props.create(Classic.class);
    }

    private final akka.actor.typed.ActorRef<Typed.Command> second =
        Adapter.spawn(getContext(), Typed.behavior(), "second");

    @Override
    public void preStart() {
      Adapter.watch(getContext(), second);
      second.tell(new Typed.Ping(Adapter.toTyped(getSelf())));
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Typed.Pong.class,
              message -> {
                Adapter.stop(getContext(), second);
              })
          .match(
              akka.actor.Terminated.class,
              t -> {
                getContext().stop(getSelf());
              })
          .build();
    }
  }
  // #classic-watch

  // #typed
  public abstract static class Typed {
    interface Command {}

    public static class Ping implements Command {
      public final akka.actor.typed.ActorRef<Pong> replyTo;

      public Ping(ActorRef<Pong> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public static class Pong {}

    public static Behavior<Command> behavior() {
      return Behaviors.receive(Typed.Command.class)
          .onMessage(
              Typed.Ping.class,
              message -> {
                message.replyTo.tell(new Pong());
                return same();
              })
          .build();
    }
  }
  // #typed

  @Test
  public void testItWorks() {
    // #create-classic
    akka.actor.ActorSystem as = akka.actor.ActorSystem.create();
    akka.actor.ActorRef classic = as.actorOf(Classic.props());
    // #create-classic
    TestProbe probe = new TestProbe(as);
    probe.watch(classic);
    probe.expectTerminated(classic, Duration.create(1, "second"));
    TestKit.shutdownActorSystem(as);
  }

  @Test
  public void testConversionFromClassicSystemToTyped() {
    // #convert-classic
    akka.actor.ActorSystem classicActorSystem = akka.actor.ActorSystem.create();
    ActorSystem<Void> typedActorSystem = Adapter.toTyped(classicActorSystem);
    // #convert-classic
    TestKit.shutdownActorSystem(classicActorSystem);
  }
}
