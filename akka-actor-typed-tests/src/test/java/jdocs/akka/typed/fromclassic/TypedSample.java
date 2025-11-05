/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed.fromclassic;

// #hello-world-actor
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import java.util.HashMap;
import java.util.Map;

// #hello-world-actor

interface TypedSample {

  // #hello-world-actor
  public class HelloWorld extends AbstractBehavior<HelloWorld.Greet> {

    public static final class Greet {
      public final String whom;
      public final ActorRef<Greeted> replyTo;

      public Greet(String whom, ActorRef<Greeted> replyTo) {
        this.whom = whom;
        this.replyTo = replyTo;
      }
    }

    public static final class Greeted {
      public final String whom;
      public final ActorRef<Greet> from;

      public Greeted(String whom, ActorRef<Greet> from) {
        this.whom = whom;
        this.from = from;
      }
    }

    public static Behavior<Greet> create() {
      return Behaviors.setup(HelloWorld::new);
    }

    private HelloWorld(ActorContext<Greet> context) {
      super(context);
    }

    @Override
    public Receive<Greet> createReceive() {
      return newReceiveBuilder().onMessage(Greet.class, this::onGreet).build();
    }

    private Behavior<Greet> onGreet(Greet command) {
      getContext().getLog().info("Hello {}!", command.whom);
      command.replyTo.tell(new Greeted(command.whom, getContext().getSelf()));
      return this;
    }
  }
  // #hello-world-actor

  // #children
  public class Parent extends AbstractBehavior<Parent.Command> {

    public interface Command {}

    public static class DelegateToChild implements Command {
      public final String name;
      public final Child.Command message;

      public DelegateToChild(String name, Child.Command message) {
        this.name = name;
        this.message = message;
      }
    }

    private static class ChildTerminated implements Command {
      final String name;

      ChildTerminated(String name) {
        this.name = name;
      }
    }

    public static Behavior<Command> create() {
      return Behaviors.setup(Parent::new);
    }

    private Map<String, ActorRef<Child.Command>> children = new HashMap<>();

    private Parent(ActorContext<Command> context) {
      super(context);
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(DelegateToChild.class, this::onDelegateToChild)
          .onMessage(ChildTerminated.class, this::onChildTerminated)
          .build();
    }

    private Behavior<Command> onDelegateToChild(DelegateToChild command) {
      ActorRef<Child.Command> ref = children.get(command.name);
      if (ref == null) {
        ref = getContext().spawn(Child.create(), command.name);
        getContext().watchWith(ref, new ChildTerminated(command.name));
        children.put(command.name, ref);
      }
      ref.tell(command.message);
      return this;
    }

    private Behavior<Command> onChildTerminated(ChildTerminated command) {
      children.remove(command.name);
      return this;
    }
  }
  // #children

  public class Child {
    public interface Command {}

    public static Behavior<Command> create() {
      return Behaviors.empty();
    }
  }
}
